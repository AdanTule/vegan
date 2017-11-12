using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbConnector.Adapter
{
    public interface IDbCommandDef
    {
        string DbCommandText { get; set; }

        CommandType DbCommandType { get; set; }

        IDbDataParameter[] DbParameters { get; set; }

    }
}
