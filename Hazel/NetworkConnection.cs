﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;


namespace Hazel
{
    /// <summary>
    ///     Abstract base class for a <see cref="Connection"/> to a remote end point via a network protocol like TCP or UDP.
    /// </summary>
    /// <threadsafety static="true" instance="true"/>
    public abstract class NetworkConnection : Connection
    {
        /// <summary>
        ///     The remote end point of this connection.
        /// </summary>
        /// <remarks>
        ///     This is the end point of the other device given as an <see cref="System.Net.EndPoint"/> rather than a generic
        ///     <see cref="ConnectionEndPoint"/> as the base <see cref="Connection"/> does.
        /// </remarks>
        public EndPoint RemoteEndPoint { get; protected set; }

        /// <summary>
        ///     The <see cref="IPMode">IPMode</see> the client is connected using.
        /// </summary>
        public IPMode IPMode { get; protected set; }

        public long GetIP4Address()
        {
            if (IPMode == IPMode.IPv4)
            {
                return ((IPEndPoint)this.RemoteEndPoint).Address.Address;
            }
            else
            {
                var bytes = ((IPEndPoint)this.RemoteEndPoint).Address.GetAddressBytes();
                return BitConverter.ToInt64(bytes, bytes.Length - 8);
            }
        }
    }
}
