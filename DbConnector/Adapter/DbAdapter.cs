using DbConnector.Tools;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbConnector.Adapter
{
    public class DbAdapter: IDbAdapter
    {

        public DbAdapter(IDbCommand dbCommand, IDbConnection dbConnection)
        {
            DbCommand = dbCommand;
            DbConnection = dbConnection;
        }

        public IDbCommand DbCommand { get; private set; }

        public IDbConnection DbConnection { get; private set; }

        int _commandTimeout = 5000;

        public int CommandTimeout
        {
            get { return _commandTimeout; }
            set { _commandTimeout = value; }
        }

        public IEnumerable<T> LoadObject<T>(IDbCommandDef commandDef) where T: class {
            try
            {
                if (commandDef == null)
                    throw new ArgumentException("Missing Db Command Def");

                List<T> itms = new List<T>();
                using (IDbConnection conn = DbConnection)
                using (IDbCommand cmd = DbCommand) {
                    if(conn.State != ConnectionState.Open) { conn.Open(); }
                    cmd.CommandType = commandDef.DbCommandType;
                    cmd.CommandText = commandDef.DbCommandText;
                    cmd.CommandTimeout = CommandTimeout;
                    cmd.Connection = conn;
                    if (commandDef.DbParameters != null) {
                        foreach (IDbDataParameter param in commandDef.DbParameters) {
                            cmd.Parameters.Add(param);
                        }
                    }
                    IDataReader reader = cmd.ExecuteReader();
                    while (reader.Read()) {
                        itms.Add(FillItem<T>(reader));
                    }
                }

                    return itms;
            }
            catch { throw; }
        }

        public IEnumerable<T> LoadObject<T>(IDbCommandDef commandDef, Func<IDataReader, T> mapper) where T : class {
            try
            {
                if (commandDef == null)
                    throw new ArgumentException("Missing Db Command Def");

                List<T> itms = new List<T>();
                using (IDbConnection conn = DbConnection)
                using (IDbCommand cmd = DbCommand)
                {
                    if (conn.State != ConnectionState.Open) { conn.Open(); }
                    cmd.CommandType = commandDef.DbCommandType;
                    cmd.CommandText = commandDef.DbCommandText;
                    cmd.CommandTimeout = CommandTimeout;
                    cmd.Connection = conn;
                    if (commandDef.DbParameters != null)
                    {
                        foreach (IDbDataParameter param in commandDef.DbParameters)
                        {
                            cmd.Parameters.Add(param);
                        }
                    }
                    IDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        itms.Add(mapper(reader));
                    }
                }

                return itms;
            }  
            catch { throw; }
        }

        public int ExecuteQuery(IDbCommandDef commandDef)
        {
            try
            {
                using (IDbConnection conn = DbConnection)
                using (IDbCommand cmd = DbCommand)
                {
                    if (conn.State != ConnectionState.Open) { conn.Open(); }
                    cmd.CommandType = commandDef.DbCommandType;
                    cmd.CommandText = commandDef.DbCommandText;
                    cmd.CommandTimeout = CommandTimeout;
                    cmd.Connection = conn;
                    if (commandDef.DbParameters != null)
                    {
                        foreach (IDbDataParameter param in commandDef.DbParameters)
                        {
                            cmd.Parameters.Add(param);
                        }
                    }

                    return cmd.ExecuteNonQuery();

                }
            }
            catch { throw; }
        }

        public object ExecuteDbScalar(IDbCommandDef commandDef) {
            try
            {
                using (IDbConnection conn = DbConnection)
                using (IDbCommand cmd = DbCommand)
                {
                    if (conn.State != ConnectionState.Open) { conn.Open(); }
                    cmd.CommandType = commandDef.DbCommandType;
                    cmd.CommandText = commandDef.DbCommandText;
                    cmd.CommandTimeout = CommandTimeout;
                    cmd.Connection = conn;
                    if (commandDef.DbParameters != null)
                    {
                        foreach (IDbDataParameter param in commandDef.DbParameters)
                        {
                            cmd.Parameters.Add(param);
                        }
                    }

                    return cmd.ExecuteScalar();
                }
            }
            catch { throw; }
        }

        protected T FillItem<T>(IDataReader reader) where T : class
        {
            ListAndArrayData lad = GetDefs<T>(reader);
            List<string> colnames = lad.ColumnNames;

            System.Reflection.PropertyInfo[] props = lad.Properties;
            T obj = Activator.CreateInstance<T>();
            foreach (System.Reflection.PropertyInfo prop in props) {
                if (colnames.Contains(prop.Name.ToLower())) {
                    if (reader[prop.Name] != DBNull.Value) {
                        if (reader[prop.Name].GetType() == typeof(decimal))
                        {
                            prop.SetValue(obj, (reader.GetDouble(prop.Name)), null);
                            //todo something
                        }
                        else {
                            prop.SetValue(obj, (reader.GetValue(reader.GetOrdinal(prop.Name)) ?? null), null);
                        }
                    }
                }
            }

            return obj;
        }

        private ListAndArrayData GetDefs<T>(IDataReader reader) where T: class
        {

            string key = typeof(T).Name;
            if (ObjCache.Instance.HasCache(key))
                return ObjCache.Instance.Get<ListAndArrayData>(key);

            ListAndArrayData lad = new ListAndArrayData
            {
                ColumnNames = reader.GetSchemaTable().Rows.Cast<DataRow>().Select(c => c["ColumnName"].ToString().ToLower()).ToList(),
                Properties = typeof(T).GetProperties()
            };

           ObjCache.Instance.Set(key, lad);
           return lad;
        }

        public class ListAndArrayData
        {
            public List<string> ColumnNames { get; set; }
            public System.Reflection.PropertyInfo[] Properties { get; set;  }
        }
    }
}
