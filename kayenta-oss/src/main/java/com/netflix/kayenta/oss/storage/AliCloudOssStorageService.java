package com.netflix.kayenta.oss.storage;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.kayenta.canary.CanaryConfig;
import com.netflix.kayenta.index.CanaryConfigIndex;
import com.netflix.kayenta.index.config.CanaryConfigIndexAction;
import com.netflix.kayenta.oss.security.AliCloudOSSAccountCredentials;
import com.netflix.kayenta.security.AccountCredentialsRepository;
import com.netflix.kayenta.storage.ObjectType;
import com.netflix.kayenta.storage.StorageService;
import com.netflix.kayenta.util.Retry;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * @author chen_muyi
 * @date 2022/7/18 19:50
 */
@Builder
@Slf4j
public class AliCloudOssStorageService implements StorageService {
    private final Retry retry = new Retry();

    public final int MAX_RETRIES = 10; // maximum number of times we'll retry an operation
    public final long RETRY_BACKOFF = 1000; // time between retries in millis
    private final static int MAX_KEYS = 1000;
    @NotNull
    private ObjectMapper objectMapper;

    @NotNull
    @Singular
    @Getter
    private List<String> accountNames;

    @Autowired
    AccountCredentialsRepository accountCredentialsRepository;

    @Autowired
    CanaryConfigIndex canaryConfigIndex;


    @Override
    public boolean servicesAccount(String accountName) {
        return accountNames.contains(accountName);
    }

    @Override
    public <T> T loadObject(String accountName, ObjectType objectType, String objectKey) throws IllegalArgumentException {
        AliCloudOSSAccountCredentials credentials = getAliCloudOSSAccountCredentials(accountName);
        String path = resolveSingularPath(objectType, objectKey, credentials);
        OSSObject object = credentials.getOssClient().getObject(credentials.getBucket(), path);
        try {
            return deserialize(object, objectType.getTypeReference());
        } catch (Exception e) {
            log.error("Failed to load {} {}", objectType.getGroup(), objectKey);
            try {
                throw e;
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to deserialize object (key: " + objectKey + ")", e);
            }
        }
    }

    private String resolveSingularPath(ObjectType objectType, String objectKey, AliCloudOSSAccountCredentials credentials) {
        String objectPath = daoRoot(credentials, objectType.getGroup() + "/" + objectKey);
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(credentials.getBucket());
        listObjectsRequest.setPrefix(objectPath);
        listObjectsRequest.setMaxKeys(100);
        ObjectListing objectListing = credentials.getOssClient().listObjects(listObjectsRequest);
        return Optional.ofNullable(objectListing)
                .map(ObjectListing::getObjectSummaries)
                .map(ossObjectSummaries -> ossObjectSummaries.get(0))
                .map(OSSObjectSummary::getKey)
                .orElseThrow(() -> new IllegalArgumentException("Unable to resolve singular " + objectType + " at " + objectPath));
    }

    private AliCloudOSSAccountCredentials getAliCloudOSSAccountCredentials(String accountName) {
        return (AliCloudOSSAccountCredentials) accountCredentialsRepository.getOne(accountName)
                .orElseThrow(() -> new IllegalArgumentException("Unable to resolve account " + accountName + "."));
    }

    @Override
    public <T> void storeObject(String accountName, ObjectType objectType, String objectKey, T obj, String filename, boolean isAnUpdate) {
        AliCloudOSSAccountCredentials credentials = getAliCloudOSSAccountCredentials(accountName);
        String path = buildOSSKey(credentials, objectType, objectKey, filename);
        ensureBucketExists(accountName);

        long updatedTimestamp = -1;
        String correlationId = null;
        String canaryConfigSummaryJson = null;
        final String originalPath;

        if (objectType == ObjectType.CANARY_CONFIG) {
            updatedTimestamp = canaryConfigIndex.getRedisTime();

            CanaryConfig canaryConfig = (CanaryConfig) obj;

            checkForDuplicateCanaryConfig(canaryConfig, objectKey, credentials);

            if (isAnUpdate) {
                // Storing a canary config while not checking for naming collisions can only be a PUT (i.e. an update to an existing config).
                originalPath = resolveSingularPath(objectType, objectKey, credentials);
            } else {
                originalPath = null;
            }

            correlationId = UUID.randomUUID().toString();

            Map<String, Object> canaryConfigSummary = new ImmutableMap.Builder<String, Object>()
                    .put("id", objectKey)
                    .put("name", canaryConfig.getName())
                    .put("updatedTimestamp", updatedTimestamp)
                    .put("updatedTimestampIso", Instant.ofEpochMilli(updatedTimestamp).toString())
                    .put("applications", canaryConfig.getApplications())
                    .build();

            try {
                canaryConfigSummaryJson = objectMapper.writeValueAsString(canaryConfigSummary);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Problem serializing canaryConfigSummary -> " + canaryConfigSummary, e);
            }

            canaryConfigIndex.startPendingUpdate(
                    credentials,
                    updatedTimestamp + "",
                    CanaryConfigIndexAction.UPDATE,
                    correlationId,
                    canaryConfigSummaryJson
            );
        } else {
            originalPath = null;
        }

        try {
            byte[] bytes = objectMapper.writeValueAsBytes(obj);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(bytes.length);
            objectMetadata.setContentMD5(new String(org.apache.commons.codec.binary.Base64.encodeBase64(DigestUtils.md5(bytes))));

            retry.retry(() -> credentials.getOssClient().putObject(
                    credentials.getBucket(),
                    path,
                    new ByteArrayInputStream(bytes),
                    objectMetadata), MAX_RETRIES, RETRY_BACKOFF);

            if (objectType == ObjectType.CANARY_CONFIG) {
                // This will be true if the canary config is renamed.
                if (originalPath != null && !originalPath.equals(path)) {
                    retry.retry(() -> credentials.getOssClient().deleteObject(credentials.getBucket(), originalPath), MAX_RETRIES, RETRY_BACKOFF);
                }

                canaryConfigIndex.finishPendingUpdate(credentials, CanaryConfigIndexAction.UPDATE, correlationId);
            }
        } catch (Exception e) {
            log.error("Update failed on path {}: {}", buildTypedFolder(credentials, objectType.getGroup()), e);

            if (objectType == ObjectType.CANARY_CONFIG) {
                canaryConfigIndex.removeFailedPendingUpdate(
                        credentials,
                        updatedTimestamp + "",
                        CanaryConfigIndexAction.UPDATE,
                        correlationId,
                        canaryConfigSummaryJson
                );
            }

            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void deleteObject(String accountName, ObjectType objectType, String objectKey) {
        AliCloudOSSAccountCredentials credentials = (AliCloudOSSAccountCredentials) accountCredentialsRepository
                .getOne(accountName)
                .orElseThrow(() -> new IllegalArgumentException("Unable to resolve account " + accountName + "."));
        OSSClient ossClient = credentials.getOssClient();
        String bucket = credentials.getBucket();
        String path = resolveSingularPath(objectType, objectKey, credentials);

        long updatedTimestamp = -1;
        String correlationId = null;
        String canaryConfigSummaryJson = null;

        if (objectType == ObjectType.CANARY_CONFIG) {
            updatedTimestamp = canaryConfigIndex.getRedisTime();

            Map<String, Object> existingCanaryConfigSummary = canaryConfigIndex.getSummaryFromId(credentials, objectKey);

            if (existingCanaryConfigSummary != null) {
                String canaryConfigName = (String) existingCanaryConfigSummary.get("name");
                List<String> applications = (List<String>) existingCanaryConfigSummary.get("applications");

                correlationId = UUID.randomUUID().toString();

                Map<String, Object> canaryConfigSummary = new ImmutableMap.Builder<String, Object>()
                        .put("id", objectKey)
                        .put("name", canaryConfigName)
                        .put("updatedTimestamp", updatedTimestamp)
                        .put("updatedTimestampIso", Instant.ofEpochMilli(updatedTimestamp).toString())
                        .put("applications", applications)
                        .build();

                try {
                    canaryConfigSummaryJson = objectMapper.writeValueAsString(canaryConfigSummary);
                } catch (JsonProcessingException e) {
                    throw new IllegalArgumentException("Problem serializing canaryConfigSummary -> " + canaryConfigSummary, e);
                }

                canaryConfigIndex.startPendingUpdate(
                        credentials,
                        updatedTimestamp + "",
                        CanaryConfigIndexAction.DELETE,
                        correlationId,
                        canaryConfigSummaryJson
                );
            }
        }
        try {
            retry.retry(() -> ossClient.deleteObject(bucket, path), MAX_RETRIES, RETRY_BACKOFF);

            if (correlationId != null) {
                canaryConfigIndex.finishPendingUpdate(credentials, CanaryConfigIndexAction.DELETE, correlationId);
            }
        } catch (Exception e) {
            log.error("Failed to delete path {}: {}", path, e);

            if (correlationId != null) {
                canaryConfigIndex.removeFailedPendingUpdate(
                        credentials,
                        updatedTimestamp + "",
                        CanaryConfigIndexAction.DELETE,
                        correlationId,
                        canaryConfigSummaryJson
                );
            }
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public List<Map<String, Object>> listObjectKeys(String accountName, ObjectType objectType, List<String> applications, boolean skipIndex) {
        AliCloudOSSAccountCredentials credentials = (AliCloudOSSAccountCredentials) accountCredentialsRepository
                .getOne(accountName)
                .orElseThrow(() -> new IllegalArgumentException("Unable to resolve account " + accountName + "."));

        if (!skipIndex && objectType == ObjectType.CANARY_CONFIG) {
            Set<Map<String, Object>> canaryConfigSet = canaryConfigIndex.getCanaryConfigSummarySet(credentials, applications);

            return Lists.newArrayList(canaryConfigSet);
        } else {
            OSSClient ossClient = credentials.getOssClient();
            String bucket = credentials.getBucket();
            String group = objectType.getGroup();
            String prefix = buildTypedFolder(credentials, group);

            ensureBucketExists(accountName);

            int skipToOffset = prefix.length() + 1;  // + Trailing slash
            List<Map<String, Object>> result = new ArrayList<>();

            log.debug("Listing {}", group);
            List<OSSObjectSummary> summaries = new ArrayList<>();
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucket, prefix, null, null, MAX_KEYS);
            while (true) {
                ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
                if (objectListing == null ||
                        CollectionUtils.isEmpty(objectListing.getObjectSummaries()) ||
                        objectListing.getObjectSummaries().size() < MAX_KEYS) {
                    break;
                }
                summaries.addAll(objectListing.getObjectSummaries());
            }

            for (OSSObjectSummary summary : summaries) {
                String itemName = summary.getKey();
                int indexOfLastSlash = itemName.lastIndexOf("/");
                Map<String, Object> objectMetadataMap = new HashMap<>();
                long updatedTimestamp = summary.getLastModified().getTime();

                objectMetadataMap.put("id", itemName.substring(skipToOffset, indexOfLastSlash));
                objectMetadataMap.put("updatedTimestamp", updatedTimestamp);
                objectMetadataMap.put("updatedTimestampIso", Instant.ofEpochMilli(updatedTimestamp).toString());

                if (objectType == ObjectType.CANARY_CONFIG) {
                    String name = itemName.substring(indexOfLastSlash + 1);

                    if (name.endsWith(".json")) {
                        name = name.substring(0, name.length() - 5);
                    }

                    objectMetadataMap.put("name", name);
                }
                result.add(objectMetadataMap);
            }
            return result;
        }
    }

    private <T> T deserialize(OSSObject ossObject, TypeReference typeReference) throws IOException {
        return objectMapper.readValue(ossObject.getObjectContent(), typeReference);
    }

    private String buildOSSKey(AliCloudOSSAccountCredentials credentials, ObjectType objectType, String objectKey, String metadataFilename) {
        if (metadataFilename == null) {
            metadataFilename = objectType.getDefaultFilename();
        }

        if (objectKey.endsWith(metadataFilename)) {
            return objectKey;
        }

        return (buildTypedFolder(credentials, objectType.getGroup()) + "/" + objectKey + "/" + metadataFilename).replace("//", "/");
    }

    private String buildTypedFolder(AliCloudOSSAccountCredentials credentials, String type) {
        return daoRoot(credentials, type).replaceAll("//", "/");
    }

    private String daoRoot(AliCloudOSSAccountCredentials credentials, String daoTypeName) {
        return credentials.getRootFolder() + '/' + daoTypeName;
    }

    public void ensureBucketExists(String accountName) {
        AliCloudOSSAccountCredentials aliCloudOSSAccountCredentials = getAliCloudOSSAccountCredentials(accountName);
        OSSClient ossClient = aliCloudOSSAccountCredentials.getOssClient();
        try {
            ossClient.getBucketInfo(aliCloudOSSAccountCredentials.getBucket());
        } catch (Exception e) {
            if (e instanceof OSSException) {
                OSSException ossException = (OSSException) e;
                if ("NoSuchBucket".equals(ossException.getErrorCode())) {
                    ossClient.createBucket(aliCloudOSSAccountCredentials.getBucket());
                }
            } else {
                e.printStackTrace();
                throw e;
            }
        }
    }

    private void checkForDuplicateCanaryConfig(CanaryConfig canaryConfig, String canaryConfigId, AliCloudOSSAccountCredentials credentials) {
        String canaryConfigName = canaryConfig.getName();
        List<String> applications = canaryConfig.getApplications();
        String existingCanaryConfigId = canaryConfigIndex.getIdFromName(credentials, canaryConfigName, applications);
        // We want to avoid creating a naming collision due to the renaming of an existing canary config.
        if (!StringUtils.isEmpty(existingCanaryConfigId) && !existingCanaryConfigId.equals(canaryConfigId)) {
            throw new IllegalArgumentException("Canary config with name '" + canaryConfigName + "' already exists in the scope of applications " + applications + ".");
        }
    }

}
