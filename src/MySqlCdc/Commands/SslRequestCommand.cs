using MySqlCdc.Constants;
using MySqlCdc.Protocol;

namespace MySqlCdc.Commands;

/// <summary>
/// SSLRequest packet used in SSL/TLS connection.
/// <a href="https://mariadb.com/kb/en/library/connection/#sslrequest-packet">See more</a>
/// </summary>
internal class SslRequestCommand(int clientCollation) : ICommand
{
    public int ClientCapabilities { get; } = (int)CapabilityFlags.LongFlag
                                             | (int)CapabilityFlags.Protocol41
                                             | (int)CapabilityFlags.SecureConnection
                                             | (int)CapabilityFlags.Ssl
                                             | (int)CapabilityFlags.PluginAuth;

    public int ClientCollation { get; } = clientCollation;
    public int MaxPacketSize { get; } = 0;

    public byte[] Serialize()
    {
        var writer = new PacketWriter();
        writer.WriteIntLittleEndian(ClientCapabilities, 4);
        writer.WriteIntLittleEndian(MaxPacketSize, 4);
        writer.WriteIntLittleEndian(ClientCollation, 1);

        // Fill reserved bytes 
        for (var i = 0; i < 23; i++)
            writer.WriteByte(0);

        return writer.CreatePacket();
    }
}