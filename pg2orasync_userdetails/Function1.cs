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

namespace pg2orasync_userdetails
{
    public static class Function1
    {
        [FunctionName("pg2orasync_userdetails")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            Root jsonpay = JsonConvert.DeserializeObject<Root>(requestBody);

            string insertQuery = "INSERT INTO hr_new.hr_user_details (username, password, department, department_id) " +
                       "VALUES (:pUName, :pPwd, :pDeptName, :pDeptNo)";
            string deleteQuery = "DELETE FROM hr_new.hr_user_details WHERE username= :pUName";
            string updateQuery = "UPDATE hr_new.hr_user_details SET password=:pPwd, department=:pDeptName, department_id=:pDeptNo WHERE username= :pUName";


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
                        cmd.Parameters.Add("pUName", jsonpay.after.username.ToString());
                        cmd.Parameters.Add("pPwd", jsonpay.after.password.ToString());
                        cmd.Parameters.Add("pDName", jsonpay.after.department.ToString());
                        cmd.Parameters.Add("pDeptNo", num);
                    }
                    else if (jsonpay.op == "u")
                    {
                        log.LogInformation("Update operation");
                        byte[] bytes = Convert.FromBase64String(jsonpay.after.department_id.ToString());
                        int num = Convert.ToInt32(bytes[0].ToString());
                        cmd = new OracleCommand(updateQuery, connection);
                        cmd.Parameters.Add("pPwd", jsonpay.after.password.ToString());
                        cmd.Parameters.Add("pDName", jsonpay.after.department.ToString());
                        cmd.Parameters.Add("pDeptNo", num);
                        cmd.Parameters.Add("pUName", jsonpay.after.username.ToString());

                    }
                    else if (jsonpay.op == "d")
                    {
                        log.LogInformation("Delete operation");
                        cmd = new OracleCommand(deleteQuery, connection);
                        cmd.Parameters.Add("pUName", jsonpay.before.username.ToString());
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

        public class Before
        {
            public string username { get; set; }
            public string password { get; set; }
            public string department { get; set; }
            public string department_id { get; set; }
        }

        public class After
        {
            public string username { get; set; }
            public string password { get; set; }
            public string department { get; set; }
            public string department_id { get; set; }
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
}

