package com.inmobi.databus;
/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.OutputStream;

public class CompressedFileReaderTest {

  public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();
    FileSystem fs;
    fs = FileSystem.getLocal(conf);

    CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
    CompressionCodec codec = codecFactory.getCodec(new Path(args[0]));
    if (codec == null) {
      System.out.println("cant find codec");
      System.exit(1);

    }
    CompressionInputStream is = codec.createInputStream(fs.open(new Path
            (args[0])));
    OutputStream out = null;
    try {
      String outputURI = CompressionCodecFactory.removeSuffix(args[0],
              codec.getDefaultExtension());
      out = fs.create(new Path(outputURI));
      org.apache.hadoop.io.IOUtils.copyBytes(is, out, conf);
    }
    finally {
      org.apache.hadoop.io.IOUtils.closeStream(out);
      IOUtils.closeStream(is);

    }
  }
}


