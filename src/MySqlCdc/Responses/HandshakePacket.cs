using MySqlCdc.Constants;
using MySqlCdc.Protocol;

namespace MySqlCdc.Packets;

/// <summary>
/// Initial handshake packet sent by the server.
/// <a href="https://mariadb.com/kb/en/library/connection/#initial-handshake-packet">See more</a>
/// </summary>
internal class HandshakePacket : IPacket
{
    public byte ProtocolVersion { get; }
    public string ServerVersion { get; }
    public long ConnectionId { get; }
    public string Scramble { get; }
    public long ServerCapabilities { get; }
    public byte ServerCollation { get; }
    public int StatusFlags { get; }
    public string Filler { get; }
    public byte AuthPluginLength { get; }
    public string AuthPluginName { get; } = String.Empty;

    public HandshakePacket(ReadOnlySpan<byte> span)
    {
        var reader = new PacketReader(span);

        ProtocolVersion = reader.ReadByte();
        ServerVersion = reader.ReadNullTerminatedString();
        ConnectionId = reader.ReadUInt32LittleEndian();
        Scramble = reader.ReadNullTerminatedString();
        var capabilityFlags1 = reader.ReadByteArraySlow(2);
        ServerCollation = reader.ReadByte();
        StatusFlags = reader.ReadUInt16LittleEndian();
        var capabilityFlags2 = reader.ReadByteArraySlow(2);
        AuthPluginLength = reader.ReadByte();
        Filler = reader.ReadString(6);
        var capabilityFlags3 = reader.ReadByteArraySlow(4);

        // Join lower and upper capability flags to a number
        var capabilityFlags = capabilityFlags1.Concat(capabilityFlags2).Concat(capabilityFlags3).ToArray();
        for (var i = 0; i < capabilityFlags.Length; i++)
        {
            ServerCapabilities |= (long)capabilityFlags[i] << (i << 3);
        }

        // Handle specific conditions
        if ((ServerCapabilities & (int)CapabilityFlags.SecureConnection) > 0)
        {
            Scramble += reader.ReadNullTerminatedString();
        }
        if ((ServerCapabilities & (int)CapabilityFlags.PluginAuth) > 0)
        {
            AuthPluginName = reader.ReadNullTerminatedString();
        }
    }
}