using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data;
using MySql.Data.MySqlClient;
using System.Diagnostics;

namespace Crster.Database.Helper
{
    [DebuggerStepThrough()]
    public class Database
    {
        private static TimeSpan? _TimeUtcOffset;
        internal static TimeSpan ServerUtcOffset
        {
            get
            {
                TimeSpan ret = new TimeSpan(0);

                if (!_TimeUtcOffset.HasValue) {
                    GetServerDateTime();
                }

                ret = _TimeUtcOffset.Value;

                return ret;
            }
        }
        internal static DateTime ConvertToLocalDateTime(DateTime dateTime)
        {
            DateTime ret = dateTime;

            if ((dateTime.Ticks + _TimeUtcOffset.Value.Ticks) > 0) {
                ret = dateTime + _TimeUtcOffset.Value;
            }

            return ret;
        }
        private static bool _HasTransactionError;
        private static MySqlTransaction _Trans;
        private static MySqlConnection _Conn;
        public static MySqlConnection ConnectionObject
        {
            get
            {
                return _Conn;
            }
        }
        /// <summary>
        /// Set database connect string
        /// </summary>
        public static string ConnectionString
        {
            get
            {
                return _Conn.ConnectionString;
            }
            set
            {
                if (_Conn != null) {
                    if (_Conn.State != System.Data.ConnectionState.Closed) {
                        EndTransact();
                    }

                    _Conn.Dispose();
                }

                _Conn = new MySqlConnection(value);
                _TimeUtcOffset = null;
                GetServerDateTime();
            }
        }
        /// <summary>
        /// Returns the real server datetime.
        /// </summary>
        /// <returns>Raw datetime of server</returns>
        public static DateTime GetServerDateTime()
        {
            DateTime ret = DateTime.MinValue;

            if (!_TimeUtcOffset.HasValue) {
                OpenConnection();
                using (MySqlCommand cmd = _Conn.CreateCommand()) {
                    cmd.CommandText = "SELECT NOW()";
                    ret = (DateTime)cmd.ExecuteScalar();
                    _TimeUtcOffset = TimeZone.CurrentTimeZone.GetUtcOffset(DateTime.Now) - TimeSpan.FromHours((ret - DateTime.UtcNow).Hours);
                }
                CloseConnection();
            }
            else {
                ret = DateTime.Now - _TimeUtcOffset.Value;
            }

            return ret;
        }
        /// <summary>
        /// Return the current datetime of server that is converted base on utcoffset of the current time
        /// </summary>
        /// <returns>Local datetime of the server</returns>
        public static DateTime GetLocalDateTime()
        {
            return ConvertToLocalDateTime(GetServerDateTime());
        }
        /// <summary>
        /// Begin transaction of any database executation. You can call this method as many as you want but be sure to accompany it with EndTransaction()
        /// </summary>
        public static void Transact()
        {
            if (_Conn.State != System.Data.ConnectionState.Open) {
                _Conn.Open();
            }

            if (_Trans == null) {
                _HasTransactionError = false;
                _Trans = _Conn.BeginTransaction();
            }
        }
        /// <summary>
        /// Commit or Rollback a transaction that was initialized by Transact() method
        /// </summary>
        public static void EndTransact()
        {
            if (_Trans != null) {

                if (_HasTransactionError) {
                    _Trans.Rollback();
                }
                else {
                    _Trans.Commit();
                }

                _Trans.Dispose();
                _Trans = null;
            }

            _HasTransactionError = false;

            if (_Conn.State != System.Data.ConnectionState.Closed) {
                _Conn.Close();
            }
        }

        private static void OpenConnection()
        {
            if (_Conn.State != System.Data.ConnectionState.Open) {
                _Conn.Open();
            }
        }
        private static void CloseConnection()
        {
            if (_Trans == null) {
                if (_Conn.State != System.Data.ConnectionState.Closed) {
                    _Conn.Close();
                }
            }
        }

        private bool _IsWithUpdate;
        private string _LastTableAccess;

        private int _ParameterIndex;
        private Dictionary<string, string> _InParameters;
        private string _Table;
        private string _Command;
        private string _SelectWith;
        private string _SelectFields;
        private string _SelectFilters;
        private string _SelectAscending;
        private string _SelectDescending;
        private string _SelectGroupBy;
        private string _SelectLimit;
        private string _SelectHaving;
        private string _SerialColumn;


        private Dictionary<string, object> _Values;

        /// <summary>
        /// Initliaze the database command
        /// </summary>
        /// <param name="databaseObject">Database object. Currently supported are stored_proc, views, table and function</param>
        public Database()
        {
            _Values = new Dictionary<string, object>();
            _InParameters = new Dictionary<string, string>();
            _ParameterIndex = 0;
        }
        public Database(string databaseObject) : this()
        {
            _Table = "`" + databaseObject + "`";
            _LastTableAccess = databaseObject;
        }
        public Database(string database, string databaseObject) :this()
        {
            _Table = "`" + database + "`." + "`" + databaseObject + "`";
            _LastTableAccess = databaseObject;
        }

        public Database Table(string tableName)
        {
            _Table = "`" + tableName + "`";
            _LastTableAccess = tableName;
            return this;
        }
        public Database Table(string tableName, string tableAlias)
        {
            _Table = "`" + tableName + "` AS `"+ tableAlias +"`";
            _LastTableAccess = tableName;
            return this;
        }
        public Database Join(string database, string leftField, string rightField)
        {
            _SelectWith += "INNER JOIN `"+ database +"` ON (`"+ database +"`.`"+ rightField +"` = `"+ _LastTableAccess +"`.`"+ leftField +"`) ";
            _LastTableAccess = database;
            return this;
        }
        public Database LeftJoin(string database, string leftField, string rightField)
        {
            _SelectWith += "LEFT JOIN `" + database + "` ON (`" + database + "`.`" + rightField + "` = `" + _LastTableAccess + "`.`" + leftField + "`) ";
            _LastTableAccess = database;
            return this;
        }
        public Database Where(string filters)
        {
            _SelectFilters = filters;
            return this;
        }
        public Database Having(string filters)
        {
            _SelectHaving = filters;
            return this;
        }
        public Database ToAscendingOrder(string field)
        {
            _SelectAscending = field;
            return this;
        }
        public Database ToDescendingOrder(string field)
        {
            _SelectDescending = field;
            return this;
        }
        public Database GroupBy(string field)
        {
            _SelectGroupBy = field;
            return this;
        }
        public Database Limit(ulong startingPosition, ulong endingPosition = 0)
        {
            _SelectLimit = startingPosition.ToString();

            if (endingPosition > 0) {
                _SelectLimit += ", " + endingPosition;
            }

            return this;
        }
        public ResultSet Select(IEnumerable<string> fields)
        {
            return Select(String.Join(",", fields.Select(item => "`" + item + "`")));
        }
        public ResultSet Select(string fields)
        {
            _SelectFields = fields;
            return ExecuteQuery();
        }
        public ResultSet SelectAll()
        {
            _SelectFields = "*";
            return ExecuteQuery();
        }
        private void FillDataTable(DataTable dt, MySqlDataReader dr)
        {
            try {

                Dictionary<string, byte> column_counter = new Dictionary<string, byte>(5);
                string column_name = String.Empty;

                for (int xx = 0; xx < dr.FieldCount; xx++) {
                    if (dt.Columns.Contains(dr.GetName(xx))) {
                        column_name = dr.GetName(xx) + ++column_counter[dr.GetName(xx)];
                    }
                    else {
                        column_counter[dr.GetName(xx)] = 0;
                        column_name = dr.GetName(xx);
                    }

                    dt.Columns.Add(column_name, dr.GetFieldType(xx));
                }

                while (dr.Read()) {
                    DataRow dt_row = dt.NewRow();

                    for (int xx = 0; xx < dr.FieldCount; xx++) {
                        object value = DBNull.Value;

                        if (!dr.IsDBNull(xx)) {
                            if (dr.GetFieldType(xx) == typeof(DateTime)) {
                                value = Database.ConvertToLocalDateTime(dr.GetDateTime(xx));
                            }
                            else {
                                value = dr.GetValue(xx);
                            }
                        }

                        dt_row[xx] = value;
                    }

                    dt.Rows.Add(dt_row);
                }
            }
            finally {
                dr.Close();
                dr.Dispose();
            }
        }
        private ResultSet ExecuteQuery()
        {
            ResultSet ret = new ResultSet(_Table);

            try {
                string query = "SELECT " + _SelectFields + " FROM " + _Table + " ";

                if (!String.IsNullOrEmpty(_SelectWith)){
                    query += _SelectWith + " ";
                }

                if (_InParameters.Count > 0) {
                    foreach (KeyValuePair<string, string> item in _InParameters) {
                        if (_SelectFilters.Contains("?" + item.Key)) {
                            _SelectFilters = _SelectFilters.Replace("?" + item.Key, "(" + item.Value + ")");
                        }
                    }
                }

                if (!String.IsNullOrEmpty(_SelectFilters)) {
                    query += "WHERE " + _SelectFilters + " ";
                }

                if (!String.IsNullOrEmpty(_SelectGroupBy)) {
                    query += "GROUP BY " + _SelectGroupBy + " ";
                }

                if (!String.IsNullOrEmpty(_SelectAscending)) {
                    query += "ORDER BY " + _SelectAscending + " ASC ";
                }
                else if (!String.IsNullOrEmpty(_SelectDescending)) {
                    query += "ORDER BY " + _SelectDescending + " DESC ";
                }

                if (!String.IsNullOrEmpty(_SelectHaving)) {
                    query += "HAVING " + _SelectHaving + " ";
                }

                if (!String.IsNullOrEmpty(_SelectLimit)) {
                    query += "LIMIT " + _SelectLimit + " ";
                }

                OpenConnection();
                using (MySqlCommand cmd = Database._Conn.CreateCommand()) {
                    cmd.CommandText = query;
                    cmd.CommandType = System.Data.CommandType.Text;

                    foreach (KeyValuePair<string, object> item in _Values) {
                        cmd.Parameters.AddWithValue("?" + item.Key, item.Value);
                    }

                    FillDataTable(ret.DataTable, cmd.ExecuteReader());
                }
            }
            catch {
                Database._HasTransactionError = true;
                throw;
            }
            finally {
                CloseConnection();
            }

            return ret;
        }

        public Database Value(string field, object value)
        {
            if (value != null) {
                if (value.GetType() == typeof(DateTime)) {
                    DateTime val_date = (DateTime)value;

                    if ((val_date.Ticks - _TimeUtcOffset.Value.Ticks) > 0) {
                        value = val_date - _TimeUtcOffset.Value;
                    }
                }
            }

            _Values[field] = value;
            _ParameterIndex++;
            return this;
        }
        public Database Value(string field, IEnumerable<object> values)
        {
            int index = 0;
            string[] param = new string[values.Count()];

            foreach (object item in values) {
                param[index] = "?" + field + "_" + _ParameterIndex;
                this.Value(param[index], item);
                index++;
            }

            _InParameters[field] = String.Join(",", param);

            return this;
        }
        public Database EnumValue(string field, object value)
        {
            _Values.Add(field, ((int)value) + 1);
            _ParameterIndex++;
            return this;
        }

        public static void ForeignKey(bool value) {
            using (MySqlCommand cmd = new MySqlCommand()) {
                cmd.CommandText = "SET FOREIGN_KEY_CHECKS = " + value;
                cmd.Connection = Database._Conn;
                cmd.ExecuteNonQuery();
            }
        }

        public uint Insert()
        {
            _Command = "INSERT INTO " + _Table;
            return Convert.ToUInt32(Execute());
        }
        public uint InsertOrUpdate()
        {
            return InsertOrUpdate(String.Empty);
        }
        public uint InsertOrUpdate(string serialColumn)
        {
            uint ret = 0;

            _IsWithUpdate = true;
            _SerialColumn = serialColumn;
            ret = Insert();
            _SerialColumn = String.Empty;
            _IsWithUpdate = false;

            return ret;
        }
        public bool Update()
        {
            _Command = "UPDATE " + _Table + " SET ";
            return (Execute() > 0);
        }
        public bool Delete()
        {
            _Command = "DELETE FROM " + _Table + " ";
            return (Execute() > 0);
        }
        private long Execute()
        {
            long ret = -1;

            string fields = "";
            string query = _Command;

            try {
                if (_InParameters.Count > 0) {
                    foreach (KeyValuePair<string, string> item in _InParameters) {
                        if (_SelectFilters.Contains("?" + item.Key)) {
                            _SelectFilters = _SelectFilters.Replace("?" + item.Key, "(" + item.Value + ")");
                        }
                    }
                }

                switch (_Command.Split(' ')[0]) {
                    case "INSERT":

                        fields = "";
                        foreach (string key in _Values.Keys) {
                            fields += "`" + key + "`" + ",";
                        }

                        query += "(" + fields.Substring(0, fields.Length - 1) + ") ";

                        fields = "";
                        foreach (string key in _Values.Keys) {
                            fields += "?" + key + ",";
                        }

                        query += "VALUES(" + fields.Substring(0, fields.Length - 1) + ") ";

                        if (_IsWithUpdate) {
                            query += "ON DUPLICATE KEY UPDATE ";

                            if (!String.IsNullOrEmpty(_SerialColumn)) {
                                query += " `" + _SerialColumn + "` = LAST_INSERT_ID(`" + _SerialColumn + "`), ";
                            }

                            fields = "";
                            foreach (string key in _Values.Keys) {
                                fields += "`" + key + "`" + "=?" + key + ", ";
                            }

                            query += fields.Substring(0, fields.Length - 2) + " ";
                        }

                        break;
                    case "UPDATE":

                        fields = "";
                        foreach (string key in _Values.Keys) {
                            fields += "`" + key + "`" + "=?" + key + ", ";
                        }

                        query += fields.Substring(0, fields.Length - 2) + " ";

                        if (!String.IsNullOrEmpty(_SelectFilters)) {
                            query += "WHERE " + _SelectFilters + " ";
                        }

                        break;
                    case "DELETE":

                        if (!String.IsNullOrEmpty(_SelectFilters)) {
                            query += "WHERE " + _SelectFilters + " ";
                        }

                        break;
                }

                OpenConnection();
                using (MySqlCommand cmd = Database._Conn.CreateCommand()) {
                    cmd.CommandText = query;
                    cmd.CommandType = System.Data.CommandType.Text;

                    foreach (KeyValuePair<string, object> item in _Values) {
                        cmd.Parameters.AddWithValue("?" + item.Key, item.Value);
                    }

                    switch (_Command.Split(' ')[0]) {
                        case "INSERT":
                            cmd.ExecuteNonQuery();
                            ret = cmd.LastInsertedId;
                            break;
                        case "UPDATE":
                        case "DELETE":
                            ret = cmd.ExecuteNonQuery();
                            break;
                    }
                }
            }
            catch {
                Database._HasTransactionError = true;
                throw;
            }
            finally {
                CloseConnection();
            }

            return ret;
        }

        public void Clear()
        {
            using (MySqlCommand cmd = Database._Conn.CreateCommand()) {
                cmd.CommandText = "SET FOREIGN_KEY_CHECKS = FALSE";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "TRUNCATE TABLE " + _Table;
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SET FOREIGN_KEY_CHECKS = TRUE";
                cmd.ExecuteNonQuery();
            }
        }
        public ResultSet ExecuteSPQuery()
        {
            ResultSet ret = new ResultSet(_Table);

            try {
                OpenConnection();
                using (MySqlCommand cmd = _Conn.CreateCommand()) {
                    cmd.CommandText = _Table;
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;

                    foreach (KeyValuePair<string, object> item in _Values) {
                        cmd.Parameters.AddWithValue("?" + item.Key, item.Value);
                    }

                    FillDataTable(ret.DataTable, cmd.ExecuteReader());
                }
            }
            catch {
                _HasTransactionError = true;
                throw;
            }
            finally {
                CloseConnection();
            }

            return ret;
        }
        public long ExecuteSPCommand()
        {
            long ret = 0;

            try {
                OpenConnection();
                using (MySqlCommand cmd = _Conn.CreateCommand()) {
                    cmd.CommandText = _Table;
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;

                    foreach (KeyValuePair<string, object> item in _Values) {
                        cmd.Parameters.AddWithValue("?" + item.Key, item.Value);
                    }

                    int res = cmd.ExecuteNonQuery();

                    if (cmd.LastInsertedId > 0) {
                        ret = cmd.LastInsertedId;
                    }
                    else {
                        ret = Convert.ToInt64(res > 0);
                    }
                }
            }
            catch {
                _HasTransactionError = true;
                throw;
            }
            finally {
                CloseConnection();
            }

            return ret;
        }
        public T ExecuteSPScalar<T>()
        {
            object ret = null;

            try {
                OpenConnection();
                using (MySqlCommand cmd = _Conn.CreateCommand()) {
                    cmd.CommandText = _Table;
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;

                    foreach (KeyValuePair<string, object> item in _Values) {
                        cmd.Parameters.AddWithValue("?" + item.Key, item.Value);
                    }

                    ret = cmd.ExecuteScalar();
                }
            }
            catch {
                _HasTransactionError = true;
                throw;
            }
            finally {
                CloseConnection();
            }

            return (T)ret;
        }
        public T ExecuteScalar<T>(string field)
        {
            object ret = null;

            try {
                string query = "SELECT `" + field + "` FROM " + _Table + " "; ;

                if (_InParameters.Count > 0) {
                    foreach (KeyValuePair<string, string> item in _InParameters) {
                        if (_SelectFilters.Contains("?" + item.Key)) {
                            _SelectFilters = _SelectFilters.Replace("?" + item.Key, "(" + item.Value + ")");
                        }
                    }
                }

                if (!String.IsNullOrEmpty(_SelectFilters)) {
                    query += "WHERE " + _SelectFilters + " ";
                }

                if (!String.IsNullOrEmpty(_SelectGroupBy)) {
                    query += "GROUP BY " + _SelectGroupBy + " ";
                }

                if (!String.IsNullOrEmpty(_SelectLimit)) {
                    query += "LIMIT " + _SelectLimit + " ";
                }

                if (!String.IsNullOrEmpty(_SelectAscending)) {
                    query += "ORDER BY " + _SelectAscending + " ASC";
                }
                else if (!String.IsNullOrEmpty(_SelectDescending)) {
                    query += "ORDER BY " + _SelectAscending + " DESC";
                }

                OpenConnection();
                using (MySqlCommand cmd = _Conn.CreateCommand()) {
                    cmd.CommandText = query;
                    cmd.CommandType = System.Data.CommandType.Text;

                    foreach (KeyValuePair<string, object> item in _Values) {
                        cmd.Parameters.AddWithValue("?" + item.Key, item.Value);
                    }

                    ret = cmd.ExecuteScalar();
                }
            }
            catch {
                _HasTransactionError = true;
                throw;
            }
            finally {
                CloseConnection();
            }

            return (T)ret;
        }
    }
}
