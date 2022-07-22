package com.netflix.kayenta.oss.security;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * @author chen_muyi
 * @date 2022/7/18 19:56
 */
@ToString
@Slf4j
public class AliCloudCredentials {

    private static String applicationVersion =
            Optional.ofNullable(AliCloudCredentials.class.getPackage().getImplementationVersion()).orElse("Unknown");

}
