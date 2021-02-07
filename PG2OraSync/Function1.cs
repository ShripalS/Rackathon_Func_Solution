using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using Newtonsoft.Json.Linq;

namespace PG2OraSync
{
    public static class Function1
    {
        [FunctionName("sync")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            Root jsonpay = JsonConvert.DeserializeObject<Root>(requestBody);

            string insertQuery = "INSERT INTO hr_new.department (department_id, department) " +
                       "VALUES (:pDeptNo, :pDName)";
            string deleteQuery = "DELETE FROM hr_new.department WHERE department_id= :pDeptNo";
            string updateQuery = "UPDATE hr_new.department SET department=:pDName WHERE department_id= :pDeptNo";


            using (OracleConnection connection = new OracleConnection("User Id=admin;Password=admin123;Data Source=database-1.cinyubrvuv64.eu-west-1.rds.amazonaws.com:1521/orcl"))
            {
                DataSet dsData = new DataSet();
                try
                {
                    OracleCommand cmd;
                    
                    if (jsonpay.op == "c")
                    {
                        log.LogInformation("Insert operation");
                        byte[] bytes = Convert.FromBase64String(jsonpay.after.department_id.ToString());
                        int num = Convert.ToInt32(bytes[0].ToString());
                        cmd = new OracleCommand(insertQuery, connection);
                        cmd.Parameters.Add("pDeptNo", num);
                        cmd.Parameters.Add("pDName", jsonpay.after.department.ToString());
                    }
                    else if (jsonpay.op=="u")
                    {
                        log.LogInformation("Update operation");
                        byte[] bytes = Convert.FromBase64String(jsonpay.after.department_id.ToString());
                        int num = Convert.ToInt32(bytes[0].ToString());
                        cmd = new OracleCommand(updateQuery, connection);
                        cmd.Parameters.Add("pDName", jsonpay.after.department.ToString());
                        cmd.Parameters.Add("pDeptNo", num);
                    }
                    else if (jsonpay.op=="d")
                    {
                        log.LogInformation("Delete operation");
                        byte[] bytes = Convert.FromBase64String(jsonpay.before.department_id.ToString());
                        int num = Convert.ToInt32(bytes[0].ToString());
                        cmd = new OracleCommand(deleteQuery, connection);
                        cmd.Parameters.Add("pDeptNo", num);
                    }
                    else
                    {
                        return new OkObjectResult("Success");
                    }
                  
                    connection.Open();
                    

                    log.LogInformation("Connected to:" + connection.ServerVersion);
                    int rowsUpdated = cmd.ExecuteNonQuery();

                    if (rowsUpdated != 0)
                    {
                        log.LogInformation("Inserted successfully");
                    }
                }
                catch (Exception ex)
                {

                    log.LogError(ex.Message.ToString());
                }
                }
                //string responseMessage = string.IsNullOrEmpty(name)
                //? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                //: $"Hello, {name}. This HTTP triggered function executed successfully.";

            return new OkObjectResult("Success");
        }
    }

    public class Before
    {
        public string department_id { get; set; }
        public object department { get; set; }
    }

    public class After
    {
        public string department_id { get; set; }
        public string department { get; set; }
    }

    public class Source
    {
        public string version { get; set; }
        public string connector { get; set; }
        public string name { get; set; }
        public long ts_ms { get; set; }
        public string snapshot { get; set; }
        public string db { get; set; }
        public string schema { get; set; }
        public string table { get; set; }
        public int txId { get; set; }
        public long lsn { get; set; }
        public object xmin { get; set; }
    }

    public class Root
    {
        public Before before { get; set; }
        public After after { get; set; }
        public Source source { get; set; }
        public string op { get; set; }
        public long ts_ms { get; set; }
        public object transaction { get; set; }
    }

}
