package main

import (
    "flag"
    "fmt"
    "log"
    "os"
    "runtime"
    "runtime/pprof"
    "syscall"

    "github.com/packing/nbpy/codecs"
    "github.com/packing/nbpy/env"
    "github.com/packing/nbpy/errors"
    "github.com/packing/nbpy/messages"
    "github.com/packing/nbpy/nnet"
    "github.com/packing/nbpy/packets"
    "github.com/packing/nbpy/utils"
)

var (
    help    bool
    version bool

    daemon   bool
    setsided bool

    addr string

    pprofFile string

    logDir   string
    logLevel = utils.LogLevelVerbose
    pidFile  string

    tcp *nnet.TCPServer = nil

    ErrorPacketNotDict = errors.Errorf("消息封包主体不是字典")
)

func usage() {
    fmt.Fprint(os.Stderr, `master

Usage: master [-hv] [-d daemon] [-f pprof file] [-a listen addr]

Options:
`)
    flag.PrintDefaults()
}

func main() {

    flag.BoolVar(&help, "h", false, "help message")
    flag.BoolVar(&version, "v", false, "print version")
    flag.BoolVar(&daemon, "d", false, "run at daemon")
    flag.BoolVar(&setsided, "s", false, "already run at daemon")
    flag.StringVar(&pprofFile, "f", "", "pprof file")
    flag.StringVar(&addr, "a", "127.0.0.1:10088", "listen addr")
    flag.Usage = usage

    flag.Parse()
    if help {
        flag.Usage()
        syscall.Exit(-1)
        return
    }
    if version {
        fmt.Println("master version 1.0")
        syscall.Exit(-1)
        return
    }

    pidFile = "./pid"

    logDir = "./logs/master"
    if !daemon {
        logDir = ""
    } else {
        if !setsided {
            utils.Daemon()
            return
        }
    }

    syscall.Unlink(pidFile)
    utils.GeneratePID(pidFile)

    var pproff *os.File = nil
    if pprofFile != "" {
        pf, err := os.OpenFile(pprofFile, os.O_RDWR|os.O_CREATE, 0644)
        if err != nil {
            log.Fatal(err)
        }
        pproff = pf
        pprof.StartCPUProfile(pproff)
    }

    defer func() {
        if pproff != nil {
            pprof.StopCPUProfile()
            pproff.Close()
        }

        utils.RemovePID(pidFile)

        utils.LogInfo(">>> master进程已退出")
    }()

    utils.LogInit(logLevel, logDir)

    //注册解码器
    env.RegisterCodec(codecs.CodecIMv2)

    //注册通信协议
    env.RegisterPacketFormat(packets.PacketFormatNB)

    messages.GlobalDispatcher.MessageObjectMapped(messages.ProtocolSchemeS2S, messages.ProtocolTagMaster, MasterMessageObject{})
    messages.GlobalDispatcher.MessageObjectMapped(messages.ProtocolSchemeS2S, messages.ProtocolTagSlave, SlaveMessageObject{})
    messages.GlobalDispatcher.MessageObjectMapped(messages.ProtocolSchemeS2S, messages.ProtocolTagAdapter, AdapterMessageObject{})
    messages.GlobalDispatcher.Dispatch()

    tcp = nnet.CreateTCPServer()
    tcp.OnDataDecoded = messages.GlobalMessageQueue.Push
    tcp.OnBye = OnBye
    err := tcp.Bind(addr, 0)
    if err != nil {
        utils.LogError("!!!无法在 %s 上启动监听", addr, err)
        tcp.Close()
    }

    utils.LogInfo(">>> 当前协程数量 > %d", runtime.NumGoroutine())

    tcp.Schedule()
    env.Schedule()

    tcp.Close()

}
