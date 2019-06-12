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
package com.azure.data.cosmos;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;

import com.azure.data.cosmos.DataType;
import com.azure.data.cosmos.IncludedPath;
import com.azure.data.cosmos.Index;
import org.testng.annotations.Test;

public class IncludedPathTest {

    @Test(groups = {"unit"})
    public void deserialize() {
        String json = "{" +
                "  'path': '\\/*'," +
                "  'indexes': [" +
                "    {" +
                "      'kind': 'Range'," +
                "      'dataType': 'String'," +
                "      'precision': -1" +
                "    }," +
                "    {" +
                "      'kind': 'Range'," +
                "      'dataType': 'Number'," +
                "      'precision': -1" +
                "    }" +
                "  ]" +
                "}";
        IncludedPath path = new IncludedPath(json);
        Collection<Index> indexes = path.indexes();
        assertThat(indexes).hasSize(2);
        assertThat(indexes).usingFieldByFieldElementComparator().contains(Index.Range(DataType.STRING, -1));
        assertThat(indexes).usingFieldByFieldElementComparator().contains(Index.Range(DataType.NUMBER, -1));
    }
}