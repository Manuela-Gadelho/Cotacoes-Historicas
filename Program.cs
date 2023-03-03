using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using Microsoft.VisualBasic.FileIO;
using Configuracoes;
using System.Data;
using System.Runtime.Intrinsics.X86;
using System.Security.Cryptography.X509Certificates;
using System.Xml;
using System.Globalization;
using System.Data.SqlClient;

namespace AtualizaCotacoesHistoricas
{
    public partial class Program
    {
        //private const string V = "CotacaoHistorica";
        public static string urlCotacaoHistorica = "https://www.b3.com.br/pesquisapregao/download?filelist=SPRD{0}.zip"; //exemplo de url: https://www.b3.com.br/pesquisapregao/download?filelist=SPRD220531.zip 
        public static string diretorioCotacao = "";
        public static string folderDestino = "";
        public static string foldertemp = "CotacaoTemp";
        public static string arquivoDestino = "pesquisa-pregao.zip";
        public static DateTime dataReferencia = DateTime.Now.AddDays(-1);
        public static string dataReferenciaString = dataReferencia.ToString("yyMMdd"); //Formato de data ano/mês/dia pegando o dia anterior ao atual. 
        public static bool Verificar = false;
        public static DateTime sysData = DateTime.Now;
        public static StreamWriter sw = new StreamWriter("C:\\TEMP\\Log_CotacoesHistoricas.txt");

        static readonly object _locker = new object();

        static void Main()
        {
            try
            {
                sw.AutoFlush = true;

                Robo verificaBloqueio = new Robo(); //Verifica se o problema está disponível e pode ser bloqueado para o início da execução
                bool programaBloqueado = false; //Se o bloqueio foi feito com sucesso, a execução é iniciada

                if (Verificar)
                {
                    programaBloqueado = true;
                }
                else
                {
                    programaBloqueado = verificaBloqueio.BloqueiaPrograma("AtualizaCotacoesHistoricas");
                }

                if (programaBloqueado)
                {
                    Console.WriteLine("AtualizaCotacoesHistoricas - Liberado para executar...");
                    Console.WriteLine("AtualizaCotacoesHistoricas - Bloqueado com sucesso na CSAG328");
                    lock (_locker) sw.WriteLine(DateTime.Now.ToString() + " - " + "AtualizaCotacoesHistoricas - Bloqueado com sucesso na CSAG328");

                    diretorioCotacao = Robo.RetornaPastaTexto();
                    folderDestino = diretorioCotacao + "CotacoesHistoricas";
                    arquivoDestino = diretorioCotacao + arquivoDestino;
                    foldertemp = diretorioCotacao + foldertemp;

                    DeletaArquivos();

                    DateTime dataPregao = ConsultaData();

                    while (DateTime.Parse(dataPregao.ToShortDateString()) < DateTime.Parse(dataReferencia.ToShortDateString()))
                    {
                        bool continua = true;

                        while (continua)
                        {
                            //Baixa o arquivo da URL (site da B3)
                            string arquivoBaixado = BaixaArquivo(urlCotacaoHistorica, arquivoDestino, dataReferenciaString);
                            Console.WriteLine("AtualizaCotacoesHistoricas - Arquivo '" + arquivoBaixado.Trim() + " ' baixado da url ' " + string.Format(urlCotacaoHistorica, dataReferenciaString).Trim() + "' com sucesso...");
                            lock (_locker) sw.WriteLine(DateTime.Now.ToString() + " - " + "AtualizaCotacoesHistoricas - Arquivo '" + arquivoBaixado.Trim() + " ' baixado da url ' " + string.Format(urlCotacaoHistorica, dataReferenciaString).Trim() + "' com sucesso...");
                            //Extrai o arquivo baixado
                            if (ExtrairArquivo(arquivoBaixado, dataReferenciaString))
                            {
                                Console.WriteLine("AtualizaCotacoesHistoricas - Arquivo extraído com sucesso...");
                                lock (_locker) sw.WriteLine(DateTime.Now.ToString() + " - " + "AtualizaCotacoesHistoricas - Arquivo extraído com sucesso...");
                                DeletaArquivos();
                                continua = false;
                            }
                            else
                            {
                                //Subtrai um dia da data atual
                                Console.WriteLine("AtualizaCotacaoHistorica - Reprocessando com uma data nova...");
                                lock (_locker) sw.WriteLine(DateTime.Now.ToString() + " - " + "AtualizaCotacaoHistorica - Reprocessando com uma data nova...");
                                dataReferencia = dataReferencia.AddDays(-1);
                                dataReferenciaString = dataReferencia.ToString("yyMMdd");
                                DeletaArquivos();
                            }
                        }
                        dataReferencia = dataReferencia.AddDays(-1);
                        dataReferenciaString = dataReferencia.ToString("yyMMdd");
                    }
                }
                else
                {
                    Console.WriteLine("AtualizaCotacoesHistoricas - Erro ao bloquear o programa.");
                    lock (_locker) sw.WriteLine(DateTime.Now.ToString() + " - " + "AtualizaCotacoesHistoricas - Erro ao bloquear o programa.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("AtualizaCotacoesHistoricas - Erro na execução do programa.");
                lock (_locker) sw.WriteLine(DateTime.Now.ToString() + " - " + "AtualizaCotacoesHistoricas - Erro na execução do programa.");
                Console.WriteLine(ex.Message);
                throw;
            }

            //Executa a gração das cotações na TACC438
            GravaCotacaoHistorica();


            Robo verificarBloqueio = new Robo(); //Verifica se o problema está bloqueado e pode ser desbloqueado para o fim da execução
            bool programaDesbloquear = false; //Se o desbloqueio foi feito com sucesso, a execução é finalizada

            if (Verificar)
            {
                programaDesbloquear = false;
            }
            else
            {
                programaDesbloquear = verificarBloqueio.DesbloqueiaPrograma("AtualizaCotacoesHistoricas");
            }
        }
        private static string BaixaArquivo(string urlCotacaoHistorica, string arquivoDestino, string dataBaixarArquivo)
        {
            urlCotacaoHistorica = string.Format(urlCotacaoHistorica, dataBaixarArquivo);

            try
            {
                //Baixa o arquivo contendo as Cotações Históricas da B3
                using (var client = new WebClient())
                {
                    client.DownloadFile(urlCotacaoHistorica, arquivoDestino);
                }
                return arquivoDestino;
            }
            catch (Exception ex)
            {
                Console.WriteLine("AtualizaCotacaoHistorica (BaixaArquivo) - Erro na execução do programa.");
                lock (_locker) sw.WriteLine(DateTime.Now.ToString() + " - " + "AtualizaCotacaoHistorica (BaixaArquivo) - Erro na execução do programa.");
                Console.WriteLine(ex.Message);
                throw;
            }
        }
        private static bool ExtrairArquivo(string arquivoOrigem, string dataReferencia)
        {
            try
            {
                System.IO.Compression.ZipFile.ExtractToDirectory(arquivoOrigem, foldertemp);

                if (Directory.GetFiles(foldertemp).Count() > 0)
                {
                    foreach (var arquivo in Directory.GetFiles(foldertemp))
                    {
                        System.IO.Compression.ZipFile.ExtractToDirectory(arquivo, folderDestino);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("AtualizaCotacaoHistorica (ExtrairAquivo) - Erro na extração do arquivo.");
                lock (_locker) sw.WriteLine(DateTime.Now.ToString() + " - " + "AtualizaCotacaoHistorica (ExtrairAquivo) - Erro na extração do arquivo.");
                Console.WriteLine(ex.Message);
                return false;

            }

        }
        private static void GravaCotacaoHistorica()
        {
            Console.WriteLine("Lendo Xml's...");

            Console.WriteLine("Inserindo dados na tabela de Cotações Históricas! Aguarde...");

            DateTime tradeDate = DateTime.Now;
            string tckrSymb = "";
            double minPric = 0;
            double maxPric = 0;

            XmlReaderSettings settings = new XmlReaderSettings();
            settings.CheckCharacters = false;

            if (Directory.GetFiles(folderDestino).Count() > 0)
            {
                foreach (var arquivo in Directory.GetFiles(folderDestino))
                {
                    using (XmlReader meuXml = XmlReader.Create(arquivo, settings))
                    {
                        meuXml.MoveToContent();

                        while (meuXml.ReadToFollowing("Dt"))
                        {
                            tradeDate = meuXml.ReadElementContentAsDateTime();

                            if (meuXml.ReadToFollowing("TckrSymb"))
                            {
                                tckrSymb = meuXml.ReadElementContentAsString();
                            }

                            if (meuXml.ReadToFollowing("MinPric"))
                            {
                                minPric = meuXml.ReadElementContentAsDouble();
                            }

                            if (meuXml.ReadToFollowing("MaxPric"))
                            {
                                maxPric = meuXml.ReadElementContentAsDouble();
                            }

                            try
                            { 
                            //Declara conexão com banco de dados
                            SQLServer sqlConnection = new SQLServer();

                            //Abre a conexão e pesquisa o registro no banco
                            sqlConnection.abrirConexao();

                            sqlConnection.executeCommand(@" 
IF EXISTS(SELECT * FROM " + sqlConnection.sDatabase + ".dbo.TACC438 WHERE PREGAO = '" + tradeDate.ToString() + @"' AND NEGOCIO  = '" + tckrSymb.Trim() + @"') BEGIN
    UPDATE " + sqlConnection.sDatabase + @".dbo.TACC438 SET 
        NEGOCIO  = '" + tckrSymb.Trim() + @"',
        PREGAO = CONVERT(DATETIME, '" + tradeDate.ToString() + @"',103),
        PRECOMAIOR = " + maxPric.ToString(CultureInfo.CreateSpecificCulture("en-US")) + @",
        PRECOMENOR = " + minPric.ToString(CultureInfo.CreateSpecificCulture("en-US")) + @" 
    WHERE 
        NEGOCIO  = '" + tckrSymb.Trim() + @"' AND PREGAO = '" + tradeDate.ToString() + @"'
END
ELSE BEGIN
    INSERT INTO " + sqlConnection.sDatabase + @".dbo.TACC438
           ([PREGAO]
           ,[NEGOCIO]
           ,[PRECOMAIOR]
           ,[PRECOMENOR])
     VALUES
           (
            CONVERT(DATETIME, '" + tradeDate.ToString() + @"',103)
           ,'" + tckrSymb.Trim() + @"'
           ," + maxPric.ToString(CultureInfo.CreateSpecificCulture("en-US")) + @"
           ," + minPric.ToString(CultureInfo.CreateSpecificCulture("en-US")) + @"
            )
     END
");

                            sqlConnection.fecharConexao();

                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("AtualizaCotacaoHistorica - Erro na inserção de dados da tabela de Cotações.");
                                lock (_locker) sw.WriteLine(DateTime.Now.ToString() + " - " + "AtualizaCotacaoHistorica - Erro na inserção de dados da tabela de Cotações.");
                                Console.WriteLine(ex.Message);
                            }
                        }
                    }

                    Console.WriteLine("Movendo para a pasta de arquivos processados, aguarde...");

                    //Mover o arquivo para outra pasta
                    if (!Directory.Exists(folderDestino+"\\Processados"))
                    {
                        Directory.CreateDirectory(folderDestino + "\\Processados");
                    }
                    if (File.Exists(arquivo.Insert(arquivo.LastIndexOf("\\"), "\\Processados")))
                    { 
                        File.Delete(arquivo.Insert(arquivo.LastIndexOf("\\"), "\\Processados"));
                    }
                    File.Move(arquivo, arquivo.Insert(arquivo.LastIndexOf("\\"), "\\Processados"));
                }
            }
        }
        private static bool DeletaArquivos()
        {
            try
            {
                System.IO.File.Delete(arquivoDestino); //Deleta arquivo .zip que foi baixado. 
                                                       //System.IO.Directory.Delete(folderDestino, true); //Deleta a pasta que criamos para descompactar o arquivo baixado. 
                if (Directory.Exists(foldertemp))
                {
                    if (Directory.GetFiles(foldertemp).Count() > 0)
                    {
                        foreach (var arquivo in Directory.GetFiles(foldertemp))
                        {
                            System.IO.File.Delete(arquivo);
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("AtualizaCotacaoHistorica (DeletaArquivos) - Erro na execução do programa.");
                lock (_locker) sw.WriteLine(DateTime.Now.ToString() + " - " + "AtualizaCotacaoHistorica (DeletaArquivos) - Erro na execução do programa.");
                Console.WriteLine(ex.Message);
                return false;
            }

        }
        static DateTime ConsultaData()
        {
            try
            {
                //Declara conexão com banco de dados
                SQLServer sqlConnection = new SQLServer();

                //Abre a conexão e pesquisa o registro no banco
                sqlConnection.abrirConexao();
                DataRow drTACC438 = sqlConnection.executeQueryFirstLine("select max (PREGAO) as PREGAO FROM " + sqlConnection.sDatabase + @".dbo.TACC438 ");

                DateTime dataPregao = System.DateTime.Now;

                //Verifica se foram encontrados registros
                if (!drTACC438["PREGAO"].ToString().Equals(""))
                {
                    dataPregao = DateTime.Parse(drTACC438["PREGAO"].ToString());
                }
                else
                {
                    dataPregao = dataPregao.AddYears(-1);
                }

                return dataPregao;
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}










