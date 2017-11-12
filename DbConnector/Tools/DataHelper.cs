using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbConnector.Tools
{
    public static class DataHelper
    {
        public static T Get<T>(this DataRow dr, int index, T defaultValue = default(T)) {
            return dr[index].Get<T>(defaultValue);
        }

        public static T Get<T>(this DataRow dr, string columnName, T defaultValue = default(T))
        {
            return dr[columnName].Get<T>(defaultValue);
        }

        static T Get<T>(this object obj, T defaultValue) {
            if(obj.IsNull()) {
                return defaultValue;
            }
            if (string.IsNullOrEmpty(obj.ToString().Trim()))
                return default(T);

            return (T)obj;
        }


        public static bool IsNull<T>(this T obj) where T: class {
            return (obj == null || obj == DBNull.Value);
        }

        public static double GetDouble(this IDataReader reader, string columnName ) {
            double dbl;
            double.TryParse(reader[columnName].ToString(), out dbl);
            return dbl;
            }

        public static int ToInt32(this object input)
        {
            int val = 0;
            int.TryParse(input.ToString(), out val);
            return val;
        }

    }
    }
