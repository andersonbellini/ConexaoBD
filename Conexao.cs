using System;
using System.Data;
using System.Data.OracleClient;
using MySql.Data.MySqlClient;
// '' <summary>
// '' Conexão com o banco de dados MySQL de GPX
// '' </summary>
// '' <remarks></remarks>
namespace Cockpit_gpx.DAL
{
    public class Conexao
    {
        private String ip_server = "10.16.100.123"; //IP Servidor BD
        private String uid = "root";  //Usuário BD
        private String pwd = "root"; //Senha BD

        private MySqlConnection cn = null;
        private OracleConnection cnOracle = null;
        private string schemmaCon = "";
        
        //  Banco de Dados Mysql
        // <summary>
        // Métodos para utilização do BD Mysql
        // </summary>
        #region BancoMysql
        private void ConexaoBD(string schemma)
        {
            if ((schemmaCon != schemma))
            {
                // Dim strCn As String = "Persist Security Info=False;server=" + ip_server + ";database=" + schemma + ";uid=" + uid + ";pwd=" + pwd + ";Pooling=false;"
                cn = new MySqlConnection((("Persist Security Info=False;server="
                                + (ip_server + (";database="
                                + (schemma + (";uid="
                                + (uid + (";pwd="
                                + (pwd + ";Pooling=false;"))))))))
                                + "default command timeout=999"));
                schemmaCon = schemma;
            }

        }

        // DATATABLE
        // '' <summary>
        // ''  Consulta que retorna um DATATABLE
        // '' </summary>
        // '' <param name="schemma">Nome do Schemma do banco de dados</param>
        // '' <param name="sqlQuery">Query para consulta</param>
        // '' <returns> Pode retornar de erro de sistema</returns>
        // '' <remarks></remarks>
        public DataTable dt(string schemma, string sqlQuery)
        {
            try
            {
                this.ConexaoBD(schemma);
                MySqlDataAdapter da = new MySqlDataAdapter(sqlQuery, cn);
                DataTable dtt = new DataTable();
                cn.Open();
                da.Fill(dtt);
                cn.Close();
                return dtt;
                // s.Tables(0)
            }
            catch (Exception erro)
            {
                cn.Close();
                throw erro;
            }

        }

        // DATASET DATASOURCE
        // '' <summary>
        // ''  Consulta que retorna um DATASET [para DATASOURCE]
        // '' </summary>
        // '' <param name="schemma">Nome do Schemma do banco de dados</param>
        // '' <param name="sqlQuery">Query para consulta</param>
        // '' <returns> Pode retornar de erro de sistema</returns>
        // '' <remarks></remarks>
        public DataSet ds(string schemma, string sqlQuery)
        {
            DataSet dst = new DataSet();
            try
            {
                this.ConexaoBD(schemma);
                MySqlDataAdapter daCust = new MySqlDataAdapter(sqlQuery, cn);
                cn.Open();
                daCust.Fill(dst);
                cn.Close();
                return dst;
            }
            catch (Exception erro)
            {
                cn.Close();
                throw erro;
            }

        }

        // Scalar Query
        // '' <summary>
        // '' Retona apenas 1 valor de apenas 1 coluna
        // '' </summary>
        // '' <param name="schemma">Nome do Schemma do banco de dados</param>
        // '' <param name="sqlQuery">Consulta do BD ( Usar Distinct ) </param>
        // '' <returns> Pode retornar de erro de sistema</returns>
        // '' <remarks></remarks>
        public string scalarQuery(string schemma, string sqlQuery)
        {
            try
            {
                this.ConexaoBD(schemma);
                MySqlDataAdapter da = new MySqlDataAdapter(sqlQuery, cn);
                DataTable dtt = new DataTable();
                cn.Open();
                da.Fill(dtt);
                cn.Close();
                return Convert.ToString(dtt.Rows[0].ItemArray[0]);  //return Convert.ToString(dtt.ItemArray[0]);  TESTAR
                // Retorna apenas 1 valor de 1 coluna
            }
            catch (Exception erro)
            {
                cn.Close();
                return ("Não foi possível realizar ! - " + erro.ToString());
            }

        }

        //  Insert
        // '' <summary>
        // '' Realiza a inserção no banco de dados
        // '' </summary>
        // '' <param name="schemma">Nome do Schemma do banco de dados</param>
        // '' <param name="sqlInsert">Query de inserção no banco de dados</param>
        // '' <returns>Pode retornar mensagem de erro se ocorrer algum erro</returns>
        // '' <remarks></remarks>
        public string Insert(string schemma, string sqlInsert)
        {
            try
            {
                this.ConexaoBD(schemma);
                MySqlCommand cmdIns = new MySqlCommand(sqlInsert, cn);
                cn.Open();
                cmdIns.ExecuteReader();
                cn.Close();
            }
            catch (Exception erro)
            {
                cn.Close();
                return ("Não foi possível realizar o INSERT - " + erro.ToString());
            }

            return "OK";
        }

        //  Update
        // '' <summary>
        // '' Realiza a atualização no banco de dados
        // '' </summary>
        // '' <param name="schemma">Nome do Schemma do banco de dados</param>
        // '' <param name="sqlUpdate">Query de atualização no banco de dados</param>
        // '' <returns>Pode retornar mensagem de erro se ocorrer algum erro</returns>
        // '' <remarks></remarks>
        public string Update(string schemma, string sqlUpdate)
        {
            try
            {
                this.ConexaoBD(schemma);
                MySqlCommand cmdIns = new MySqlCommand(sqlUpdate, cn);
                cn.Open();
                cmdIns.ExecuteReader();
                cn.Close();
            }
            catch (Exception erro)
            {
                cn.Close();
                return ("Não foi possível realizar o UPDATE - " + erro.ToString());
            }

            return "OK";
        }

        //  Delete
        // '' <summary>
        // '' [CUIDADO] Apaga DEFINITIVAMENTE os dados do banco de dados
        // '' </summary>
        // '' <param name="schemma">Nome do Schemma do banco de dados</param>
        // '' <param name="sqlDelete">[*CUIDADO*] Query para apagar do banco de dados [*CUIDADO*]</param>
        // '' <returns>Pode retornar mensagem de erro se ocorrer algum erro</returns>
        // '' <remarks></remarks>
        public string Delete(string schemma, string sqlDelete)
        {
            try
            {
                this.ConexaoBD(schemma);
                MySqlCommand cmdIns = new MySqlCommand(sqlDelete, cn);
                cn.Open();
                cmdIns.ExecuteReader();
                cn.Close();
            }
            catch (Exception erro)
            {
                cn.Close();
                return ("Não foi possível realizar o Delete - " + erro.ToString());
            }

            return "OK";
        }

        //  CreateTable
        // '' <summary>
        // '' Realiza a atualização no banco de dados
        // '' </summary>
        // '' <param name="schemma">Nome do Schemma do banco de dados</param>
        // '' <param name="sqlCreate">Query de criação de tabela no banco de dados</param>
        // '' <returns>Pode retornar mensagem de erro se ocorrer algum erro</returns>
        // '' <remarks></remarks>
        public string CreateTable(string schemma, string sqlCreate)
        {
            try
            {
                this.ConexaoBD(schemma);
                MySqlCommand cmdCreate = new MySqlCommand(sqlCreate, cn);
                cn.Open();
                cmdCreate.ExecuteReader();
                cn.Close();
            }
            catch (Exception erro)
            {
                cn.Close();
                return ("Não foi possível realizar o CREATE TABLE - " + erro.ToString());
            }

            return "OK";
        }

        #endregion

        //  Banco de dados Oracle
        // <summary>
        // Métodos para utilização do BD Oracle
        // </summary>
        #region BancoOracle

        private void ConexaoBDOracle()
        {
            cnOracle = new OracleConnection(("Data Source=PR16.SJK.EMB;User ID=ckm_man;password=pcp_ckm; Connection " + "Lifetime=1;"));
        }

        // Scalar Query Oracle
        // '' <summary>
        // '' Retona apenas 1 valor de apenas 1 coluna
        // '' </summary>
        // '' <param name="OraclesqlQuery">Consulta do BD ( Usar Distinct ) </param>
        // '' <returns> Pode retornar de erro de sistema</returns>
        // '' <remarks></remarks>
        public string Z_Oracle_scalarQuery(string OraclesqlQuery)
        {
            try
            {
                this.ConexaoBDOracle();
                OracleDataAdapter da = new OracleDataAdapter(OraclesqlQuery, cnOracle);
                DataTable dt = new DataTable();
                cnOracle.Open();
                da.Fill(dt);
                cnOracle.Close();
                return Convert.ToString(dt.Rows[0].ItemArray[0]); //(0).ItemArray[0]); testar aqui!!!  // Retorna apenas 1 valor de 1 coluna
            }
            catch (Exception erro)
            {
                cnOracle.Close();
                return ("Não foi possível realizar ! - " + erro.ToString());
            }

        }

        // DATATABLE ORACLE
        // '' <summary>
        // ''  Consulta que retorna um DATATABLE
        // '' </summary>
        // '' <param name="OraclesqlQuery">Query para consulta</param>
        // '' <returns> Pode retornar de erro de sistema</returns>
        // '' <remarks></remarks>
        public DataTable Z_Oracle_dt(string OraclesqlQuery)
        {
            try
            {
                this.ConexaoBDOracle();
                OracleDataAdapter da = new OracleDataAdapter(OraclesqlQuery, cnOracle);
                DataTable dt = new DataTable();
                cnOracle.Open();
                da.Fill(dt);
                cnOracle.Close();
                return dt;
            }
            catch (Exception erro)
            {
                cnOracle.Close();
                throw erro;
            }

        }

        // DATASET DATASOURCE ORACLE
        // '' <summary>
        // ''  Consulta que retorna um DATASET [para DATASOURCE]
        // '' </summary>
        // '' <param name="OraclesqlQuery">Query para consulta</param>
        // '' <returns> Pode retornar de erro de sistema</returns>
        // '' <remarks></remarks>
        public DataSet Z_Oracle_ds(string OraclesqlQuery)
        {
            DataSet dst = new DataSet();
            try
            {
                this.ConexaoBDOracle();
                OracleDataAdapter daCust = new OracleDataAdapter(OraclesqlQuery, cnOracle);
                cnOracle.Open();
                daCust.Fill(dst);
                cnOracle.Close();
                return dst;
            }
            catch (Exception erro)
            {
                cnOracle.Close();
                throw erro;
            }

        }

        #endregion
    }

}
