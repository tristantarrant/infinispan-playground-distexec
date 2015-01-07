package net.dataforte.infinispan.playground.distexec;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.remoting.transport.Address;

public class SimpleTask implements DistributedCallable<Address, Map<String, Object>, String>, Serializable {
   char ch;
   int count;

   @Override
   public String call() throws Exception {
      StringBuilder s = new StringBuilder();
      for (int i = 0; i < count; i++) {
         s.append(ch);
      }
      return s.toString();
   }

   @Override
   public void setEnvironment(Cache<Address, Map<String, Object>> cache, Set<Address> inputKeys) {
      Address address = cache.getCacheManager().getAddress();
      Map<String, Object> parameters = cache.get(address);
      ch = (char) parameters.get("char");
      count = (int) parameters.get("count");
   }

}
