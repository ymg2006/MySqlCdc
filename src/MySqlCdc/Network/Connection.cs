using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using MySqlCdc.Constants;

namespace MySqlCdc.Network;

internal class Connection
{
    private readonly ReplicaOptions _options;
    public Stream Stream { get; private set; }

    public Connection(ReplicaOptions options)
    {
        _options = options;
        Exception? ex = null;

        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        foreach (var ip in Dns.GetHostAddresses(options.Hostname))
        {
            try
            {
                socket.Connect(new IPEndPoint(ip, _options.Port));
                Stream = new NetworkStream(socket);
            }
            catch (Exception e)
            {
                ex = e;
            }
        }

        if (Stream == null)
            throw ex ?? new InvalidOperationException("Could not connect to the server");
    }

    public async Task WritePacketAsync(byte[] array, byte seqNum, CancellationToken cancellationToken = default)
    {
        var header = new byte[PacketConstants.HeaderSize];

        // Write header size
        for (var i = 0; i < PacketConstants.HeaderSize - 1; i++)
        {
            header[i] = (byte)(0xFF & ((uint)array.Length >> (i << 3)));
        }

        // Write sequence number
        header[3] = seqNum;

        await Stream.WriteAsync(header, 0, header.Length, cancellationToken);
        await Stream.WriteAsync(array, 0, array.Length, cancellationToken);
    }

    /// <summary>
    /// In sequential mode packet type is determined by calling client code.
    /// We don't use System.IO.Pipelines as it cannot determine packet type.
    /// </summary>
    public async Task<(byte[], byte)> ReadPacketAsync(CancellationToken cancellationToken = default)
    {
        var header = new byte[PacketConstants.HeaderSize];
        var headerReadLen = await Stream.ReadAsync(header, 0, header.Length, cancellationToken);

        if (headerReadLen != 0)
        {
            // We don't care about packet splitting in handshake flow
            var bodySize = header[0] + (header[1] << 8) + (header[2] << 16);
            var body = new byte[bodySize];
            var bodyReadLen = await Stream.ReadAsync(body, 0, body.Length, cancellationToken);

            if (bodyReadLen != 0)
            {
                return (body, header[3]);
            }
        }
        return ([], 0);
    }

    public void UpgradeToSsl()
    {
        var sslStream = new SslStream(Stream, false, ValidateServerCertificate, null);
        sslStream.AuthenticateAsClient(_options.Hostname);
        Stream = sslStream;
    }

    private bool ValidateServerCertificate(object sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors sslPolicyErrors)
    {
        if (_options.SslMode == SslMode.IfAvailable || _options.SslMode == SslMode.Require)
            return true;

        if (sslPolicyErrors == SslPolicyErrors.None)
            return true;

        return false;
    }
}