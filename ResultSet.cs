using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Diagnostics;

namespace Crster.Database.Helper
{
    [DebuggerStepThrough()]
    public class ResultSet
    {
        private int _Index;
        public DataTable DataTable { get; internal set; }
        public int RecordCounts
        {
            get
            {
                return DataTable.Rows.Count;
            }
        }

        public ResultSet(string resultName)
        {
            _Index = -1;
            DataTable = new DataTable(resultName);
        }
        public ResultSet(DataTable data)
        {
            _Index = -1;
            if (data == null) {
                throw new ArgumentNullException("data");
            }
            DataTable = data;
        }

        public void ResetIndex()
        {
            _Index = -1;
        }
        public bool Read()
        {
            if (DataTable == null) {
                return false;
            }

            if (_Index >= (DataTable.Rows.Count - 1)) {
                return false;
            }
            else{
                _Index++;
                return true;
            }
        }

        public T Value<T>(string column)
        {
            object ret = ConvertDbNullToNull(column);
            return (T)ret;
        }
        private object ConvertDbNullToNull(string column)
        {
            object ret = DataTable.Rows[_Index][column];

            if (ret == DBNull.Value) {
                ret = null;
            }

            return ret;
        }

        public bool HasValue(string column)
        {
            return DataTable.Rows[_Index][column] != DBNull.Value;
        }
        public bool GetBoolean(string column)
        {
            return Convert.ToBoolean(ConvertDbNullToNull(column));
        }
        public byte GetByte(string column)
        {
            return Convert.ToByte(ConvertDbNullToNull(column));
        }
        public sbyte GetSbyte(string column)
        {
            return Convert.ToSByte(ConvertDbNullToNull(column));
        }
        public short GetShort(string column)
        {
            return Convert.ToInt16(ConvertDbNullToNull(column));
        }
        public ushort GetUshort(string column)
        {
            return Convert.ToUInt16(ConvertDbNullToNull(column));
        }
        public int GetInteger(string column)
        {
            return Convert.ToInt32(ConvertDbNullToNull(column));
        }
        public uint GetUinteger(string column)
        {
            return Convert.ToUInt32(ConvertDbNullToNull(column));
        }
        public long GetLong(string column)
        {
            return Convert.ToInt64(ConvertDbNullToNull(column));
        }
        public ulong GetUlong(string column)
        {
            return Convert.ToUInt64(ConvertDbNullToNull(column));
        }
        public float GetFloat(string column)
        {
            return Convert.ToSingle(ConvertDbNullToNull(column));
        }
        public double GetDouble(string column)
        {
            return Convert.ToDouble(ConvertDbNullToNull(column));
        }
        public decimal GetDecimal(string column)
        {
            return Convert.ToDecimal(ConvertDbNullToNull(column));
        }
        public DateTime GetDateTime(string column)
        {
            return Convert.ToDateTime(ConvertDbNullToNull(column));
        }
        public string GetString(string column)
        {
            return Convert.ToString(ConvertDbNullToNull(column));
        }
        public string GetFromHash(string colum)
        {
            string ret = null;

            string result = GetString(colum);

            if (result != "") {
                byte[] arr = Convert.FromBase64String(result);
                ret = Encoding.Default.GetString(arr);
            }

            return ret;
        }
        public static string GetHash(string data)
        {
            string ret = null;

            try {
                byte[] arr = Encoding.Default.GetBytes(data);
                ret = Convert.ToBase64String(arr);
            }
            catch{
                ret = null;
            }

            return ret;
        }

        public T EnumValue<T>(string column)
        {
            object value = ConvertDbNullToNull(column);

            if(value.GetType() == typeof(string)){
                return (T)Enum.Parse(typeof(T), value as string);
            }
            else{
                return (T)Enum.ToObject(typeof(T), value);
            }
        }
    }
}
