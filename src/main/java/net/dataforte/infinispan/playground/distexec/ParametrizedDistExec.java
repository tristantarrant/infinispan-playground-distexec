package net.dataforte.infinispan.playground.distexec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.Address;

public class ParametrizedDistExec {

   public static void main(String[] args) throws Exception {
      int expectedNodeCount = (args.length == 0) ? 2 : Integer.parseInt(args[0]);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      ConfigurationBuilder config = new ConfigurationBuilder();

      DefaultCacheManager cm = new DefaultCacheManager(global.build(), config.build());
      ConfigurationBuilder paramConfig = new ConfigurationBuilder();
      paramConfig.clustering().cacheMode(CacheMode.DIST_SYNC);
      cm.defineConfiguration("parameters", paramConfig.build());
      cm.start();
      Cache<Address, Map<String, ?>> parameters = cm.getCache("parameters");
      if (cm.isCoordinator()) {
         System.out.printf("Waiting for %d nodes...", expectedNodeCount);
         while (cm.getClusterSize() < expectedNodeCount) {
            Thread.sleep(1000);
         }
         char ch = 'A';
         Random random = new Random();
         for (Address address : cm.getMembers()) {
            Map<String, Object> nodeParameters = new HashMap<String, Object>();
            nodeParameters.put("char", ch);
            nodeParameters.put("count", random.nextInt(100) + 1);
            parameters.put(address, nodeParameters);
            ch++;
         }
         DefaultExecutorService des = new DefaultExecutorService(parameters);
         List<Future<String>> results = des.submitEverywhere(new SimpleTask());
         for(Future<String> result : results) {
            System.out.printf("Result: %s\n", result.get());
         }
      }  else {
         System.out.println("Slave node waiting for Map/Reduce tasks.  Ctrl-C to exit.");
         LockSupport.park();
      }

      cm.stop();
   }
}
