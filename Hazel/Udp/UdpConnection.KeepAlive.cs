﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;


namespace Hazel.Udp
{
    partial class UdpConnection
    {
        /// <summary>
        ///     The interval from data being received or transmitted to a keepalive packet being sent in milliseconds.
        /// </summary>
        /// <remarks>
        ///     <para>
        ///         Keepalive packets serve to close connections when an endpoint abruptly disconnects and to ensure than any
        ///         NAT devices do not close their translation for our argument. By ensuring there is regular contact the
        ///         connection can detect and prevent these issues.
        ///     </para>
        ///     <para>
        ///         The default value is 10 seconds, set to System.Threading.Timeout.Infinite to disable keepalive packets.
        ///     </para>
        /// </remarks>
        public int KeepAliveInterval
        {
            get
            {
                return keepAliveInterval;
            }

            set
            {
                keepAliveInterval = value;
                
                //Update timer
                ResetKeepAliveTimer();
            }
        }
        int keepAliveInterval = 10000;

        /// <summary>
        ///     The timer creating keepalive pulses.
        /// </summary>
        Timer keepAliveTimer;

        /// <summary>
        ///     Lock for keep alive timer.
        /// </summary>
        Object keepAliveTimerLock = new Object();
        
        /// <summary>
        ///     Starts the keepalive timer.
        /// </summary>
        void InitializeKeepAliveTimer()
        {
            lock (keepAliveTimerLock)
            {
                keepAliveTimer = new Timer(
                    SendKeepAlivePacket,
                    null,
                    keepAliveInterval,
                    keepAliveInterval
                );
            }
        }

        void SendKeepAlivePacket(object state)
        {
            try
            {
                ReliableSend((byte)UdpSendOption.Ping);
                Trace.WriteLine("Keepalive packet sent.");
            }
            catch (Exception e)
            {
                Trace.WriteLine("Keepalive packet failed to send: " + e.Message);
                DisposeKeepAliveTimer();
            }
        }

        /// <summary>
        ///     Resets the keepalive timer to zero.
        /// </summary>
        void ResetKeepAliveTimer()
        {
            lock (keepAliveTimerLock)
            {
                if (keepAliveTimer != null)
                {
                    keepAliveTimer.Change(keepAliveInterval, keepAliveInterval);
                }
            }
        }

        /// <summary>
        ///     Disposes of the keep alive timer.
        /// </summary>
        void DisposeKeepAliveTimer()
        {
            lock (keepAliveTimerLock)
            {
                if (keepAliveTimer != null)
                {
                    keepAliveTimer.Dispose();
                    keepAliveTimer = null;
                }
            }
        }
    }
}
