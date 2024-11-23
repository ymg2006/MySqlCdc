using MySqlCdc.Protocol;

namespace MySqlCdc.Commands;

internal class AuthPluginSwitchCommand(string password, string scramble, string authPluginName) : ICommand
{
    public string Password { get; } = password;
    public string Scramble { get; } = scramble;
    public string AuthPluginName { get; } = authPluginName;

    public byte[] Serialize()
    {
        var writer = new PacketWriter();
        writer.WriteByteArray(Extensions.GetEncryptedPassword(Password, Scramble, AuthPluginName));
        return writer.CreatePacket();
    }
}