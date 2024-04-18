using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Threading;
using CrystalDecisions.CrystalReports.Engine;
using CrystalDecisions.Shared;
using System.IO;
using System.IO.Compression;
using MulPackage;
using FileStream;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using SQLProcedure;
using ServerHTTP;

namespace Worker
{
    public class Worker : HandlerContext
    {
        public StateWorkerEnum State;
        private MulPackage.Report RPT;
        public MulPackage.SendMessage Logger;
        private List<HttpListenerContext> Queue = new List<HttpListenerContext>();
        public Dictionary<InfoWorkerEnum, object> Info = new Dictionary<InfoWorkerEnum, object>();
        public List<string> Events = new List<string>();
        private ManualResetEvent Waiting = new ManualResetEvent(false);
        private ConnectionInfo CredentialsSQL;

        public bool ConnectLogger()
        {
            if (Logger != null && Logger.Connect()) return true;
            return false;
        }
        public void Add(HttpListenerContext HttpCon)
        {
            // Metodo encargado de agregar una nueva conexion a la lista
            if (Queue == null) Queue = new List<HttpListenerContext>();
            if (State == StateWorkerEnum.Runing) { Queue.Add(HttpCon); Waiting.Set(); }
        }
        public void Remove(HttpListenerContext HttpCon)
        {
            // Metodo encargado de agregar una nueva conexion a la lista
            if (Queue != null) if (Queue.Contains(HttpCon)) Queue.Remove(HttpCon);
        }
        public void Finish()
        {
            // Permite finalizar el servicio de respuestas borrando y cerrando las conexiones pendientes.
            State = StateWorkerEnum.Stoped;
            EmptyQueue();
            RegisterEvent("Se finalizo un hilo de generacion de documentos");
        }
        public void EmptyQueue()
        {
            // Util en caso de errores, permite limpiar la cola y cerrar las conexiones
            // Esto no reperara el error, solo limpiara todo para evitar solicitudes largas
            foreach (HttpListenerContext HttpCon in Queue) try { HttpCon.Response.Close(); } catch { }
            Queue.Clear();
            RegisterEvent("Se eliminaron todas las peticiones pendientes");
        }
        private void Generator()
        {
            // Bucle iterador, obtiene las consultas de la lista una a una y en caso de finalizar se 
            // mantendra a la espera de una nueva conexion
            while (State == StateWorkerEnum.Runing)
            {
                try { 
                    HttpListenerContext Current = Collector(); // Al obtener la conexion se elimina de la lista
                    HttpListenerResponse Response = Current.Response;
                    HttpListenerRequest Request = Current.Request;
                    // Obtenemos la informacion del documento solicitado
                    JsonRequest.Request RequestData = JsonSerializer.Deserialize<JsonRequest.Request>(Request.InputStream);
                    string Type = RequestData.Tipo;
                    string Serie = RequestData.Serie;
                    string RFC = RequestData.RFC;
                    int Folio = RequestData.Folio;
                    string Query = string.Format(Invoice.GetDocInvoce, Info[InfoWorkerEnum.Table], 
                        // Campos de la seleccion
                        Folio,  Serie, RFC);
                    // Obtener informacion
                    // Query almacenada en SQLProcedure.dll
                    // Informacion devuelta en orden: DocNum, U_UUID, DocEntry
                    object[] Data = SQL.Excute(CredentialsSQL, Query).First();
                    string FileName = string.Format("{0}_{1}_{2}", Serie, Folio, Data[(int)ColName.UUID]);
                    // Generar documentos
                    PDF pdf = new PDF().Generate(RPT, (int)Data[(int)ColName.DocEntry], FileName );
                    XML xml = new XML().Generate((int)Data[(int)ColName.DocEntry], FileName, CredentialsSQL);
                    ZIP zip = new ZIP().Comprime(pdf, xml, FileName);
                    // Adjuntamos el documento y el nombre del mismo
                    Response.AddHeader("Cookie", $"fileName=\"{FileName}.zip\";");
                    zip.Content().CopyTo(Response.OutputStream);
                    Response.OutputStream.Flush();
                    // Responder
                    Response.Close();
                    // Enviar el evento al visor
                    Logger.Send("Se envio: " + FileName);
                }
                catch (Exception ex) {
                    RegisterEvent(ex.Message);
                    Logger.Send(ex.Message);
                }
            }
        }
        private void RegisterEvent(string Message) {
            /* 
             Por cuestiones de memoria los eventos estaran limitados a 200.
             La lista de eventos sera necesaria para cuando el usuario cierre el 
             programa de administracion poder cargarlos desde el worker.
            */
            
            if (Events.Count() >= 200) Events.Remove(Events.First());
            Events.Add(Message);
        }
        public Worker(int Type, string Path, string SQLTable, ConnectionInfo Conn, ref MulPackage.SendMessage Logger)
        {
            State = StateWorkerEnum.Stoped;
            Info.Add(InfoWorkerEnum.Type, Type);
            Info.Add(InfoWorkerEnum.Path, Path);
            Info.Add(InfoWorkerEnum.InLine, Queue.Count());
            Info.Add(InfoWorkerEnum.Events, Events);
            Info.Add(InfoWorkerEnum.Table, SQLTable);
            CredentialsSQL = Conn;
            this.Logger = Logger;

            // Objetos externos
            RPT = new Report(Info[InfoWorkerEnum.Path].ToString(), CredentialsSQL);

        }
        public void Initialize()
        {
            // Inicializamos el hilo de ejecucion, esto como metodo del thread start
            try { Waiting.Set(); } catch { }
            State = StateWorkerEnum.Runing;
            Generator();
        }
        private HttpListenerContext Collector()
        {
            HttpListenerContext Current = null;
            // Obtenemos la primera instancia
            if (Queue.Count() == 0) Waiting.WaitOne();
            Current = Queue.First();
            Queue.Remove(Current);
            // Devolvemos la instancia de la conexion 
            return Current;
        }
    }

    public enum StateWorkerEnum
    {
        Runing,
        Stoped
    }

    public enum InfoWorkerEnum
    {
        Type,
        Table,
        Path,
        InLine,
        Events
    }


}