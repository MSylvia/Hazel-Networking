﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;


namespace Hazel.Udp
{
    /// <summary>
    ///     Listens for new UDP connections and creates UdpConnections for them.
    /// </summary>
    /// <inheritdoc />
    public class UdpConnectionListener : NetworkConnectionListener
    {
        /// <summary>
        ///     The socket listening for connections.
        /// </summary>
        Socket listener;

        /// <summary>
        ///     Buffer to store incoming data in.
        /// </summary>
        byte[] dataBuffer = new byte[ushort.MaxValue];

        /// <summary>
        ///     The connections we currently hold
        /// </summary>
        ConcurrentDictionary<EndPoint, UdpServerConnection> clients = new ConcurrentDictionary<EndPoint, UdpServerConnection>();

        /// <summary>
        ///     Creates a new UdpConnectionListener for the given <see cref="IPAddress"/>, port and <see cref="IPMode"/>.
        /// </summary>
        /// <param name="IPAddress">The IPAddress to listen on.</param>
        /// <param name="port">The port to listen on.</param>
        /// <param name="mode">The <see cref="IPMode"/> to listen with.</param>
        [Obsolete("Temporary constructor in beta only, use NetworkEndPoint constructor instead.")]
        public UdpConnectionListener(IPAddress IPAddress, int port, IPMode mode = IPMode.IPv4)
            : this (new NetworkEndPoint(IPAddress, port, mode))
        {

        }

        /// <summary>
        ///     Creates a new UdpConnectionListener for the given <see cref="IPAddress"/>, port and <see cref="IPMode"/>.
        /// </summary>
        /// <param name="endPoint">The endpoint to listen on.</param>
        public UdpConnectionListener(NetworkEndPoint endPoint)
        {
            this.EndPoint = endPoint.EndPoint;
            this.IPMode = endPoint.IPMode;

            if (endPoint.IPMode == IPMode.IPv4)
                this.listener = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            else
            {
                if (!Socket.OSSupportsIPv6)
                    throw new HazelException("IPV6 not supported!");

                this.listener = new Socket(AddressFamily.InterNetworkV6, SocketType.Dgram, ProtocolType.Udp);
                this.listener.SetSocketOption(SocketOptionLevel.IPv6, (SocketOptionName)27, false);
            }
        }

        ~UdpConnectionListener()
        {
            this.Dispose(false);
        }

        /// <inheritdoc />
        public override void Start()
        {
            try
            {
                try
                {
                    listener.Bind(EndPoint);
                }
                catch
                {
                    System.Threading.Thread.Sleep(10);
                    listener.Bind(EndPoint);
                }
            }
            catch (SocketException e)
            {
                throw new HazelException("Could not start listening as a SocketException occured", e);
            }

            StartListeningForData();
        }

        /// <summary>
        ///     Instructs the listener to begin listening.
        /// </summary>
        void StartListeningForData()
        {
            EndPoint remoteEP = EndPoint;
            
            try
            {
                if (listener == null) return;
                listener.BeginReceiveFrom(dataBuffer, 0, dataBuffer.Length, SocketFlags.None, ref remoteEP, ReadCallback, dataBuffer);
            }
            catch (NullReferenceException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (SocketException)
            {
                //Client no longer reachable, pretend it didn't happen
                //TODO possibly able to disconnect client, see other TODO
                StartListeningForData();
                return;
            }
        }

        /// <summary>
        ///     Called when data has been received by the listener.
        /// </summary>
        /// <param name="result">The asyncronous operation's result.</param>
        void ReadCallback(IAsyncResult result)
        {
            int bytesReceived;
            EndPoint remoteEndPoint = new IPEndPoint(IPMode == IPMode.IPv4 ? IPAddress.Any : IPAddress.IPv6Any, 0);

            //End the receive operation
            try
            {
                bytesReceived = listener.EndReceiveFrom(result, ref remoteEndPoint);
            }
            catch (NullReferenceException)
            {
                //If the socket's been disposed then we can just end there.
                return;
            }
            catch (ObjectDisposedException)
            {
                //If the socket's been disposed then we can just end there.
                return;
            }
            catch (SocketException)
            {
                //Client no longer reachable, pretend it didn't happen
                //TODO should this not inform the connection this client is lost???

                //This thread suggests the IP is not passed out from WinSoc so maybe not possible
                //http://stackoverflow.com/questions/2576926/python-socket-error-on-udp-data-receive-10054

                StartListeningForData();
                return;
            }

            // Exit if no bytes read, we've closed.
            if (bytesReceived == 0)
                return;

            byte[] buffer;
            try
            {
                //Copy to new buffer
                buffer = new byte[bytesReceived];
                Buffer.BlockCopy((byte[])result.AsyncState, 0, buffer, 0, bytesReceived);
            }
            finally
            {
                //Begin receiving again
                StartListeningForData();
            }

            bool added = false;
            UdpServerConnection connection;
            if (!this.clients.TryGetValue(remoteEndPoint, out connection))
            {
                lock (this.clients)
                {
                    if (!this.clients.TryGetValue(remoteEndPoint, out connection))
                    {
                        // If this is a new client then connect with them!
                        // Check for malformed connection attempts
                        if (buffer[0] != (byte)UdpSendOption.Hello)
                            return;

                        connection = this.clients.GetOrAdd(remoteEndPoint, (key) => { added = true; return new UdpServerConnection(this, remoteEndPoint, IPMode); });
                    }
                }
            }

            //Inform the connection of the buffer (new connections need to send an ack back to client)
            connection.HandleReceive(buffer);

            if (added)
            {
                byte[] dataBuffer = new byte[buffer.Length - 3];
                Buffer.BlockCopy(buffer, 3, dataBuffer, 0, buffer.Length - 3);
                InvokeNewConnection(dataBuffer, connection);
            }
        }

        /// <summary>
        ///     Sends data from the listener socket.
        /// </summary>
        /// <param name="bytes">The bytes to send.</param>
        /// <param name="endPoint">The endpoint to send to.</param>
        internal void SendData(byte[] bytes, int length, EndPoint endPoint)
        {
            if (length > bytes.Length) return;

            try
            {
                listener.BeginSendTo(
                    bytes,
                    0,
                    length,
                    SocketFlags.None,
                    endPoint,
                    delegate (IAsyncResult result)
                    {
                        listener.EndSendTo(result);
                    },
                    null
                );
            }
            catch (SocketException e)
            {
                throw new HazelException("Could not send data as a SocketException occured.", e);
            }
            catch (ObjectDisposedException)
            {
                //Keep alive timer probably ran, ignore
                return;
            }
        }

        /// <summary>
        ///     Sends data from the listener socket.
        /// </summary>
        /// <param name="bytes">The bytes to send.</param>
        /// <param name="endPoint">The endpoint to send to.</param>
        internal void SendDataSync(byte[] bytes, int length, EndPoint endPoint)
        {
            try
            {
                listener.SendTo(
                    bytes,
                    0,
                    length,
                    SocketFlags.None,
                    endPoint
                );
            }
            catch (SocketException e)
            {
                throw new HazelException("Could not send data as a SocketException occured.", e);
            }
            catch (ObjectDisposedException)
            {
                //Keep alive timer probably ran, ignore
                return;
            }
        }

        /// <summary>
        ///     Removes a virtual connection from the list.
        /// </summary>
        /// <param name="endPoint">The endpoint of the virtual connection.</param>
        internal void RemoveConnectionTo(EndPoint endPoint)
        {
            UdpServerConnection conn;
            this.clients.TryRemove(endPoint, out conn);
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            foreach (var connection in this.clients.Values)
            {
                if (connection.State == ConnectionState.Connected)
                {
                    try
                    {
                        connection.SendDisconnect();
                    }
                    catch { }
                }

                connection.Dispose();
            }

            this.clients.Clear();


            if (listener != null)
            {
                listener.Shutdown(SocketShutdown.Both);
                listener.Close();
                this.listener.Dispose();
                this.listener = null;
            }

            base.Dispose(disposing);
        }
    }
}
