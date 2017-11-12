using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Caching;

namespace DbConnector.Tools
{
    public sealed class ObjCache
    {
        private static readonly ObjCache _instance = new ObjCache();

        public ObjCache() { }

        static ObjCache() { }

        public static ObjCache Instance { get { return _instance;  } }


        readonly ObjectCache cache = MemoryCache.Default;


        public T Get<T>(string key) {

            if (cache.Contains(key))
                return (T)cache[key];

            return default(T);
        }

        public void Set<T>(string key, T value, int expireInMinutes = 15) {
            cache.Set(key, value, DateTime.Now.AddMinutes(expireInMinutes));
        }

        public bool HasCache(string key)
        {
            return cache.Contains(key);
        }
    }
}
