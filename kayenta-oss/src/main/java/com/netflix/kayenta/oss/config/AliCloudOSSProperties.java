package com.netflix.kayenta.oss.config;

import com.netflix.kayenta.security.AccountCredentials;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * @author chen_muyi
 * @date 2022/7/19 9:26
 */
@Data
@ConfigurationProperties("kayenta.oss")
public class AliCloudOSSProperties {
    private String name;
    private String bucket;
    private String accessKeyId;
    private String accessKeySecret;
    private String region;
    private String rootFolder;
    private String endPoint;
    private List<AccountCredentials.Type> supportedTypes;
}
