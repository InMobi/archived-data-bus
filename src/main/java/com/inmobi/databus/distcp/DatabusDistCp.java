package com.inmobi.databus.distcp;

import com.sun.org.apache.xml.internal.serializer.utils.StringToIntTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class DatabusDistCp {
     private static final Log LOG = LogFactory.getLog(DatabusDistCp.class);

  private void doDistCp(String categoryList, String destination, String fileListDirectory) throws Exception {
      Set<String> categories = getCategories(categoryList);

  }

    private Set<String> getCategories(String categoryList) {
        Set<String> categories = new HashSet<String>();
        StringTokenizer tokenizer = new StringTokenizer(categoryList, ",");
        while(tokenizer.hasMoreTokens()) {
            categories.add(tokenizer.nextToken().trim());
        }
        return categories;
    }

  public static void main(String[] args) {
      if (args == null || args.length < 3) {
          LOG.info("Incorrect usage");
          return;
      }
      //comma seperate category list eg: c1, c2, .., cn
      String catgeoryList = args[1].trim();
      //fully qualified hdfs dest path
      String destination = args[2].trim();
      //directory to read the files from
      String fileListDirectory = args[3].trim();

    
  }
}
