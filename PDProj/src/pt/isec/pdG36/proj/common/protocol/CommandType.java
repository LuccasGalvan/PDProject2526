package pt.isec.pdG36.proj.common.protocol;

public enum CommandType {
    REGISTER,
    HEARTBEAT,
    UNREGISTER,
    GET_SERVER,
    SERVER_RESPONSE,
    PRIMARY_RESPONSE,
    NO_SERVER
}