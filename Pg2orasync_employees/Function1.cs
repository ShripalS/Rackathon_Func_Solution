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

namespace Pg2orasync_employees
{
    public static class Function1
    {
        [FunctionName("pg2orasync_employees")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            Root jsonpay = JsonConvert.DeserializeObject<Root>(requestBody);

            string insertQuery = "INSERT INTO hr_new.employees (employee_id, first_name, last_name, email, phone_number, job_id, salary, manager_id, department, department_id) " +
                       "VALUES (:pEmpId, :pFirstName, :pLastName, :pEmail, :pPhNo, :pJobId, :pSalary, :pManagerId, :pDep, :pDepId)";
            string deleteQuery = "DELETE FROM hr_new.employees WHERE employee_id= :pEmpId";
            string updateQuery = "UPDATE hr_new.employees SET first_name=:pFirstName, last_name=:pLastName, email=:pEmail, phone_number=:pPhNo, job_id=:pJobId, salary=:pSalary, " +
                "manager_id=:pManagerId, department=:pDep, department_id=:pDepId WHERE employee_id = :pEmpId";
            //string updateQuery = "UPDATE hr_new.employees SET first_name=:pFirstName, last_name=:pLastName, email=:pEmail WHERE employee_id = :pEmpId";

            //            string updateQuery = "UPDATE hr_new.employees " +
            //"SET first_name = 'manish12'," +
            //"last_name = 'agrawal12'," +
            //"email = 'email', " +
            //"phone_number = 555, " +
            //"job_id = 26, " +
            //"salary = 1000, " +
            //"manager_id = 124, " +
            //"department = 'RCL', " +
            //"department_id = 1 " +
            //"WHERE employee_id = 6" ;

            //string updateQuery = "UPDATE hr_new.employees SET first_name=:pFirstName WHERE employee_id = :pEmpId";

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
                        cmd.Parameters.Add("pEmpId", jsonpay.after.employee_id);
                        cmd.Parameters.Add("pFirstName", jsonpay.after.first_name.ToString());
                        cmd.Parameters.Add("pLastName", jsonpay.after.last_name.ToString());
                        cmd.Parameters.Add("pEmail", jsonpay.after.email.ToString());
                        cmd.Parameters.Add("pPhNo", jsonpay.after.phone_number.ToString());
                        //cmd.Parameters.Add("pHireDate", DateTime.Now.ToString());
                        cmd.Parameters.Add("pJobId", jsonpay.after.job_id.ToString());
                        cmd.Parameters.Add("pSalary", jsonpay.after.salary);
                        cmd.Parameters.Add("pManagerId", jsonpay.after.manager_id);
                        cmd.Parameters.Add("pDep", jsonpay.after.department.ToString());
                        cmd.Parameters.Add("pDepId", num);
                    }
                    else if (jsonpay.op == "u")
                    {
                        log.LogInformation("Update operation");
                        byte[] bytes = Convert.FromBase64String(jsonpay.after.department_id.ToString());
                        int num = Convert.ToInt32(bytes[0].ToString());
                        cmd = new OracleCommand(updateQuery, connection);
                        cmd.Parameters.Add("pFirstName", jsonpay.after.first_name.ToString());
                        cmd.Parameters.Add("pLastName", jsonpay.after.last_name.ToString());
                        cmd.Parameters.Add("pEmail", jsonpay.after.email.ToString());
                        cmd.Parameters.Add("pPhNo", jsonpay.after.phone_number.ToString());
                        //cmd.Parameters.Add("pHireDate", jsonpay.after.hire_date.ToString());
                        cmd.Parameters.Add("pJobId", jsonpay.after.job_id.ToString());
                        cmd.Parameters.Add("pSalary", jsonpay.after.salary);
                        cmd.Parameters.Add("pManagerId", jsonpay.after.manager_id);
                        cmd.Parameters.Add("pDep", jsonpay.after.department.ToString());
                        cmd.Parameters.Add("pDepId", num);
                        cmd.Parameters.Add("pEmpId", jsonpay.after.employee_id);
                    }
                    else if (jsonpay.op == "d")
                    {
                        log.LogInformation("Delete operation");
                        cmd = new OracleCommand(deleteQuery, connection);
                        cmd.Parameters.Add("pEmpId", jsonpay.before.employee_id);
                    }
                    else
                    {
                        return new OkObjectResult("Success");
                    }

                    connection.Open();

                    log.LogInformation("Query", cmd.ToString());
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
        public int employee_id { get; set; }
        public string first_name { get; set; }
        public string last_name { get; set; }
        public string email { get; set; }
        public string phone_number { get; set; }
        //public string hire_date { get; set; }
        public string job_id { get; set; }
        public string salary { get; set; }
        public string manager_id { get; set; }
        public string department { get; set; }
        public string department_id { get; set; }
    }

    public class After
    {
        public int employee_id { get; set; }
        public string first_name { get; set; }
        public string last_name { get; set; }
        public string email { get; set; }
        public string phone_number { get; set; }
        //public long hire_date { get; set; }
        public string job_id { get; set; }
        public double salary { get; set; }
        public int manager_id { get; set; }
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

