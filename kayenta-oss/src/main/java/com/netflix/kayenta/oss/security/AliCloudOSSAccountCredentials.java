package com.netflix.kayenta.oss.security;

import com.aliyun.oss.OSSClient;
import com.aliyuncs.auth.BasicCredentials;
import com.aliyuncs.auth.BasicSessionCredentials;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.netflix.kayenta.security.AccountCredentials;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * @author chen_muyi
 * @date 2022/7/18 19:56
 */
@Builder
@Data
public class AliCloudOSSAccountCredentials implements AccountCredentials<BasicCredentials> {

    @NotNull
    private String name;

    @NotNull
    @Singular
    private List<Type> supportedTypes;

    @NotNull
    private BasicCredentials credentials;

    private String bucket;
    private String region;
    private String rootFolder;

    @Override
    public String getType() {
        return "aliCloud-OSS";
    }

    @JsonIgnore
    private OSSClient ossClient;
}
