package com.netflix.kayenta.oss.config;

import com.aliyun.oss.OSSClient;

import com.aliyuncs.auth.BasicCredentials;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.kayenta.oss.security.AliCloudOSSAccountCredentials;
import com.netflix.kayenta.oss.storage.AliCloudOssStorageService;
import com.netflix.kayenta.security.AccountCredentials;
import com.netflix.kayenta.security.AccountCredentialsRepository;
import com.netflix.kayenta.security.MapBackedAccountCredentialsRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

/**
 * @author chen_muyi
 * @date 2022/7/18 20:16
 */
@Configuration
@ConditionalOnProperty(value = "kayenta.oss.enabled",havingValue = "true")
@ComponentScan({"com.netflix.kayenta.oss"})
@EnableConfigurationProperties(AliCloudOSSProperties.class)
@Slf4j
public class OSSConfiguration {
    @Autowired
    ObjectMapper kayentaObjectMapper;

    @Bean
    @ConditionalOnMissingBean(AccountCredentialsRepository.class)
    AccountCredentialsRepository accountCredentialsRepository() {
        return new MapBackedAccountCredentialsRepository();
    }

    @Bean
    public boolean registerAliCloudCredentials(AliCloudOSSProperties aliCloudOSSProperties,AccountCredentialsRepository accountCredentialsRepository){
        OSSClient ossClient = new OSSClient(aliCloudOSSProperties.getEndPoint(), aliCloudOSSProperties.getAccessKeyId(), aliCloudOSSProperties.getAccessKeySecret());
        AliCloudOSSAccountCredentials aliCloudOSSAccountCredentials = AliCloudOSSAccountCredentials.builder()
                .bucket(aliCloudOSSProperties.getBucket())
                .credentials(new BasicCredentials(aliCloudOSSProperties.getAccessKeyId(), aliCloudOSSProperties.getAccessKeySecret()))
                .region(aliCloudOSSProperties.getRegion())
                .rootFolder(aliCloudOSSProperties.getRootFolder())
                .supportedTypes(aliCloudOSSProperties.getSupportedTypes())
                .ossClient(ossClient)
                .build();
        accountCredentialsRepository.save(aliCloudOSSProperties.getName(),aliCloudOSSAccountCredentials);
        return true;
    }

    @Bean
    @DependsOn({"registerAliCloudCredentials"})
    public AliCloudOssStorageService initStorageService(AccountCredentialsRepository accountCredentialsRepository){
        AliCloudOssStorageService.AliCloudOssStorageServiceBuilder builder = AliCloudOssStorageService.builder();
        accountCredentialsRepository
                .getAll()
                .stream()
                .filter(c -> c instanceof AliCloudOSSAccountCredentials)
                .filter(c -> c.getSupportedTypes().contains(AccountCredentials.Type.OBJECT_STORE))
                .map(AccountCredentials::getName)
                .forEach(builder::accountName);
        AliCloudOssStorageService storageService = builder.objectMapper(kayentaObjectMapper).build();

        log.info("Populated AliCloudOssStorageService with {} aliCloud accounts.", storageService.getAccountNames().size());
        return storageService;
    }
}
