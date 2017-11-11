using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbConnector.Adapter
{
    public interface IDbAdapter
    {
        IEnumerable<T> LoadObject<T>(IDbCommandDef commandDef) where T : class;
        IEnumerable<T> LoadObject<T>(IDbCommandDef commandDef, Func<IDataReader, T> mapper) where T: class;
        int ExecuteQuery(IDbCommandDef commandDef);
        object ExecuteDbScalar(IDbCommandDef commandDef);
    }
}
