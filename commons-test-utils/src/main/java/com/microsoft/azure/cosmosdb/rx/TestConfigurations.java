/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.cosmosdb.rx;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Strings;

/**
 * Contains the configurations for tests.
 * 
 * For running tests, you can pass a customized endpoint configuration in one of the following
 * ways:
 * <ul>
 * <li>-DACCOUNT_KEY="[your-key]" -ACCOUNT_HOST="[your-endpoint]" as JVM
 * command-line option.</li>
 * <li>You can set ACCOUNT_KEY and ACCOUNT_HOST as environment variables.</li>
 * </ul>
 * 
 * If none of the above is set, emulator endpoint will be used.
 */
public final class TestConfigurations {
    // Replace MASTER_KEY and HOST with values from your Azure Cosmos DB account.
    // The default values are credentials of the local emulator, which are not used in any production environment.
    // <!--[SuppressMessage("Microsoft.Security", "CS002:SecretInNextLine")]-->
    public static String MASTER_KEY =
            System.getProperty("ACCOUNT_KEY", 
                    StringUtils.defaultString(Strings.emptyToNull(
                            System.getenv().get("ACCOUNT_KEY")),
                            "hoTaUW11OCQ16P4Qp7tUM3C4xk20vTQZbh0eSTZHwTPyAQNMwlYqim63ltJJLuhUeDLC081r34vHOn8PHlGmug=="));

    public static String HOST =
            System.getProperty("ACCOUNT_HOST",
                    StringUtils.defaultString(Strings.emptyToNull(
                            System.getenv().get("ACCOUNT_HOST")),
                            "https://cosmos-sdk-tests.documents.azure.com:443/"));

    public static String CONSISTENCY =
            System.getProperty("ACCOUNT_CONSISTENCY",
                               StringUtils.defaultString(Strings.emptyToNull(
                                       System.getenv().get("ACCOUNT_CONSISTENCY")), "Strong"));

    public static String PREFERRED_LOCATIONS =
            System.getProperty("PREFERRED_LOCATIONS",
                               StringUtils.defaultString(Strings.emptyToNull(
                                       System.getenv().get("PREFERRED_LOCATIONS")), null));

    public static String MAX_RETRY_LIMIT =
            System.getProperty("MAX_RETRY_LIMIT",
                               StringUtils.defaultString(Strings.emptyToNull(
                                       System.getenv().get("MAX_RETRY_LIMIT")),
                                                         "2"));

    public static String DESIRED_CONSISTENCIES =
            System.getProperty("DESIRED_CONSISTENCIES",
                               StringUtils.defaultString(Strings.emptyToNull(
                                       System.getenv().get("DESIRED_CONSISTENCIES")),
                                                         null));

    public static String PROTOCOLS =
            System.getProperty("PROTOCOLS",
                               StringUtils.defaultString(Strings.emptyToNull(
                                       System.getenv().get("PROTOCOLS")),
                                                         null));
}
