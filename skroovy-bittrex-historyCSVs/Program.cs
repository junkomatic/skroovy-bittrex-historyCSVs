using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Data;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Data.SQLite;

namespace skroovy_bittrex_historyCSVs
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                RunAsync().Wait();
            }
            catch (Exception e)
            {
                Console.Write("\r\n\r\n!!!!TOP LVL ERR>> " + e.InnerException.Message + ",\r\n\t" + e.InnerException.Message);
                Console.ReadLine();
            }

            Console.WriteLine("\r\n-Press ENTER to exit-");
            Console.ReadLine();
        }

        static async Task RunAsync()
        {
            await new HistoricalData().UpdateHistData();
        }
    }



    public class HistoricalData
    {
        private static ConcurrentQueue<HistDataResponse> DataQueue = new ConcurrentQueue<HistDataResponse>();
        private int downloaded = 0,
                    saved = 0,
                    totalCount = 0;
        private bool savedComplete = false;
        private const string dbName = "MarketHistory.data";


        public async Task UpdateHistData()
        {
            Console.WriteLine();

            //Start ProcessQueue thread:
            var SQLthread = new Thread(() => ProcessDataQueue());
            SQLthread.IsBackground = true;
            SQLthread.Name = "Saving-Market-Histories";
            SQLthread.Start();


            //Call list of available markets from Btrex:
            GetMarketsResponse markets = await BtrexREST.GetMarkets();
            if (markets.success != true)
            {
                Console.WriteLine("    !!!!ERR GET-MARKETS>>> " + markets.message);
                return;
            }


            //Create string list of BTC market deltas only: 
            List<string> BTCmarketDeltas = new List<string>();
            foreach (GetMarketsResult market in markets.result)
            {
                if (market.MarketName.Split('-')[0] == "BTC")
                {
                    BTCmarketDeltas.Add(market.MarketName);
                }
            }

            totalCount = BTCmarketDeltas.Count;

            var downloadHists = BTCmarketDeltas.Select(EnqueueData).ToArray();
            await Task.WhenAll(downloadHists);

            while (!savedComplete)
                Thread.Sleep(100);

            SQLthread.Abort();
            Console.WriteLine();

            Console.WriteLine("Updating CSVs - Just a moment...");
            UpdateOrCreateCSVs();

            Console.WriteLine("DONE");
        }


        private async Task EnqueueData(string delta)
        {
            HistDataResponse histData = await BtrexREST.GetMarketHistoryV2(delta);
            if (histData.success != true)
            {
                Console.WriteLine("    !!!!ERR GET-HISTORY>>> " + histData.message);
                return;
            }
            downloaded++;

            DataQueue.Enqueue(histData);
            //Console.Write("\rMarkets Downloaded: {0}/{1}/{2}", downloaded, saved, totalCount);
        }


        private void ProcessDataQueue()
        {
            bool newDB = checkNewDB();

            using (SQLiteConnection conn = new SQLiteConnection("Data Source=" + dbName + ";Version=3;"))
            {
                conn.Open();
                using (var cmd = new SQLiteCommand(conn))
                {
                    using (var tx = conn.BeginTransaction())
                    {
                        do
                        {
                            while (DataQueue.IsEmpty)
                            {
                                Thread.Sleep(100);
                            }

                            bool DQed = DataQueue.TryDequeue(out HistDataResponse history);
                            if (DQed)
                            {
                                if (newDB)
                                    CreateNewDataTable(history, cmd);
                                else
                                    UpdateDataTable(history, cmd);
                            }

                        }
                        while (saved < totalCount);
                        tx.Commit();
                    }
                }
                conn.Close();
            }
            savedComplete = true;
        }


        private void CreateNewDataTable(HistDataResponse data, SQLiteCommand cmd)
        {
            cmd.CommandText = string.Format("CREATE TABLE IF NOT EXISTS {0} (DateTime TEXT, Open TEXT, Close TEXT, Low TEXT, High TEXT, Volume TEXT)", data.MarketDelta);
            cmd.ExecuteNonQuery();

            foreach (HistDataLine line in data.result)
            {
                EnterSQLiteRow(line, cmd, data.MarketDelta);
            }

            saved++;
            Console.Write("\rDOWNLOADING MARKETS: {0}/{1}", saved, totalCount);
        }


        private void UpdateDataTable(HistDataResponse data, SQLiteCommand cmd)
        {
            //look for market. if !exist then CreateNewDataTable()
            cmd.CommandText = string.Format("SELECT CASE WHEN tbl_name = '{0}' THEN 1 ELSE 0 END FROM sqlite_master WHERE tbl_name = '{0}' AND type = 'table'", data.MarketDelta);
            if (!Convert.ToBoolean(cmd.ExecuteScalar()))
            {
                CreateNewDataTable(data, cmd);
                return;
            }

            cmd.CommandText = string.Format("SELECT * FROM {0} ORDER BY datetime(DateTime) DESC Limit 1", data.MarketDelta);
            DateTime dt = Convert.ToDateTime(cmd.ExecuteScalar());

            foreach (HistDataLine line in data.result)
            {
                if (line.T <= dt)
                {
                    continue;
                }
                else
                {
                    EnterSQLiteRow(line, cmd, data.MarketDelta);
                }
            }
            saved++;
            Console.Write("\rDOWNLOADING MARKETS: {0}/{1}", saved, totalCount);
        }

    
        private void EnterSQLiteRow(HistDataLine line, SQLiteCommand cmd, string delta)
        {
            cmd.CommandText = string.Format(
                                    "INSERT INTO {0} (DateTime, Open, High, Low, Close, Volume) "
                                    + "VALUES ('{1}', '{2}', '{3}', '{4}', '{5}', '{6}')",
                                    delta,
                                    line.T.ToString("yyyy-MM-dd HH:mm:ss"), line.O, line.H, line.L, line.C, line.V);

            cmd.ExecuteNonQuery();
        }


        private bool checkNewDB()
        {
            if (!File.Exists(dbName))
            {
                Console.WriteLine("LOCAL DATA FILE NOT FOUND\r\nCREATING NEW '{0}' FILE...", dbName);
                SQLiteConnection.CreateFile(dbName);
                return true;
            }
            else
                return false;
        }


        public void UpdateOrCreateCSVs()
        {
            using (SQLiteConnection conn = new SQLiteConnection("Data Source=" + dbName + ";Version=3;"))
            {
                conn.Open();
                using (var cmd = new SQLiteCommand(conn))
                {
                    List<string> tableNames = new List<string>();
                    cmd.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY 1";
                    SQLiteDataReader r = cmd.ExecuteReader();
                    while (r.Read())
                    {
                        tableNames.Add(r["name"].ToString());
                    }

                    Directory.CreateDirectory("BtrexCSVs");

                    foreach (string tName in tableNames)
                    {
                        DataTable dt = new DataTable();
                        using (var sqlAdapter = new SQLiteDataAdapter("SELECT * from " + tName, conn))
                            sqlAdapter.Fill(dt);

                        string path = @"BtrexCSVs\" + tName + ".csv";

                        if (!File.Exists(path))
                        {
                            GenerateNewCSV(dt, path);
                        }
                        else
                        {
                            UpdateExistingCSV(dt, path);
                        }
                    }
                }
            }
        }


        private void GenerateNewCSV(DataTable table, string path)
        {
            IEnumerable<string> colHeadings = table.Columns.OfType<DataColumn>().Select(col => col.ColumnName);
            using (StreamWriter writer = File.AppendText(path))
            {
                writer.WriteLine(string.Join(",", colHeadings));

                foreach (DataRow row in table.Rows)
                    writer.WriteLine(string.Join(",", row.ItemArray));
            }
        }


        private void UpdateExistingCSV(DataTable table, string path)
        {
            DateTime lastDateTime = Convert.ToDateTime(File.ReadLines(path).Last().Split(',')[0]);
            using (StreamWriter writer = File.AppendText(path))
            {
                foreach (DataRow row in table.Rows)
                {
                    DateTime rowTime = Convert.ToDateTime(row.ItemArray[0]);
                    //Console.WriteLine(rowTime + " : " + lastDateTime);
                    //Console.ReadLine();

                    if (rowTime <= lastDateTime)
                        continue;
                    else
                        writer.WriteLine(string.Join(",", row.ItemArray));
                }
            }
        }

    }


    public static class BtrexREST
    {
        private static HttpClient client = new HttpClient()
        {
            BaseAddress = new Uri("https://bittrex.com/api/v1.1/"),
            Timeout = TimeSpan.FromMinutes(3)
        };

        public static async Task<GetMarketsResponse> GetMarkets()
        {
            GetMarketsResponse marketsResponse = null;
            HttpResponseMessage response = await client.GetAsync("public/getmarkets");
            if (response.IsSuccessStatusCode)
                marketsResponse = await response.Content.ReadAsAsync<GetMarketsResponse>();
            return marketsResponse;
        }

        public static async Task<HistDataResponse> GetMarketHistoryV2(string delta)
        {
            HttpRequestMessage mesg = new HttpRequestMessage()
            {
                RequestUri = new Uri("https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=" + delta + "&tickInterval=fiveMin", UriKind.Absolute),
                Method = HttpMethod.Get
            };

            HistDataResponse history = null;
            HttpResponseMessage response = await client.SendAsync(mesg);
            if (response.IsSuccessStatusCode)
                history = await response.Content.ReadAsAsync<HistDataResponse>();
            else if (response.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable)
            {
                do
                {
                    Thread.Sleep(50);
                    response = await client.SendAsync(mesg);
                }
                while (response.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable);
            }
            else
                Console.WriteLine("FAIL:  " + response.ReasonPhrase);

            history.MarketDelta = delta.Replace('-', '_');

            return history;
        }
    }








    public class HistDataResponse
    {
        public bool success { get; set; }
        public string message { get; set; }
        public List<HistDataLine> result { get; set; }
        public string MarketDelta { get; set; }
    }
    public class HistDataLine
    {
        public decimal O { get; set; }
        public decimal H { get; set; }
        public decimal L { get; set; }
        public decimal C { get; set; }
        public decimal V { get; set; }
        public DateTime T { get; set; }
    }


    public class GetMarketsResponse
    {
        public bool success { get; set; }
        public string message { get; set; }
        public List<GetMarketsResult> result { get; set; }
    }
    public class GetMarketsResult
    {
        public string MarketCurrency { get; set; }
        public string BaseCurrency { get; set; }
        public string MarketCurrencyLong { get; set; }
        public string BaseCurrencyLong { get; set; }
        public decimal MinTradeSize { get; set; }
        public string MarketName { get; set; }
        public bool IsActive { get; set; }
        public string Created { get; set; }
    }
    
}
