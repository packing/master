package main

import (
    "strings"

    "github.com/packing/nbpy/codecs"
    "github.com/packing/nbpy/messages"
    "github.com/packing/nbpy/nnet"
    "github.com/packing/nbpy/utils"
)


func OnBye(c nnet.Controller) error {
    if c.GetTag() == messages.ProtocolTagAdapter {
        notifyAdapterBye(c.GetSessionID())
        delAdapter(c.GetSessionID())
    } else if c.GetTag() == messages.ProtocolTagSlave {
        notifySlaveBye(c.GetSessionID())
        delSlave(c.GetSessionID())
    } else if c.GetTag() == messages.ProtocolTagClient {
        delGateway(c.GetSessionID())
    }
    return nil
}

func notifySlaveCome(sessionid nnet.SessionID, si SlaveInfo) {
    var targets = make([]nnet.SessionID, 0)
    adapterLock.Lock()
    for k := range GlobalAdapters {
        targets = append(targets, k)
    }
    adapterLock.Unlock()

    msg := messages.CreateS2SMessage(messages.ProtocolTypeSlaveCome)

    a := make(codecs.IMMap)
    a[messages.ProtocolKeySessionId] = sessionid
    a[messages.ProtocolKeyId] = si.pid
    a[messages.ProtocolKeyUnixAddr] = si.unixAddr
    a[messages.ProtocolKeyHost] = si.host
    a[messages.ProtocolKeyValue] = si.vmFree

    msg.SetTag(messages.ProtocolTagAdapter)
    msg.SetBody(a)

    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        for _, v := range targets {
            tcp.Send(v, pck)
        }
    }
}

func notifyAdapterCome(sessionid nnet.SessionID, ai AdapterInfo) {

    var targets = make([]nnet.SessionID, 0)
    adapterLock.Lock()
    for k := range GlobalGateways {
        targets = append(targets, k)
    }
    adapterLock.Unlock()

    msg := messages.CreateS2SMessage(messages.ProtocolTypeAdapterCome)

    a := make(codecs.IMMap)
    a[messages.ProtocolKeySessionId] = sessionid
    a[messages.ProtocolKeyId] = ai.pid
    a[messages.ProtocolKeyUnixAddr] = ai.unixAddr
    a[messages.ProtocolKeyHost] = ai.host
    a[messages.ProtocolKeyUnixMsgAddr] = ai.unixMsgAddr
    a[messages.ProtocolKeyValue] = ai.connection

    msg.SetTag(messages.ProtocolTagClient)
    msg.SetBody(a)

    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        for _, v := range targets {
            tcp.Send(v, pck)
        }
    }
}

func notifySlaveChange(sessionid nnet.SessionID) {

    si := getSlave(sessionid)
    if si == nil {
        return
    }
    var targets = make([]nnet.SessionID, 0)
    adapterLock.Lock()
    for k := range GlobalAdapters {
        targets = append(targets, k)
    }
    adapterLock.Unlock()

    msg := messages.CreateS2SMessage(messages.ProtocolTypeSlaveChange)

    a := make(codecs.IMMap)
    a[messages.ProtocolKeySessionId] = sessionid
    a[messages.ProtocolKeyId] = si.pid
    a[messages.ProtocolKeyUnixAddr] = si.unixAddr
    a[messages.ProtocolKeyHost] = si.host
    a[messages.ProtocolKeyValue] = si.vmFree

    msg.SetTag(messages.ProtocolTagAdapter)
    msg.SetBody(a)

    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        for _, v := range targets {
            tcp.Send(v, pck)
        }
    }
}

func notifyAdapterChange(sessionid nnet.SessionID) {

    ai := getAdapter(sessionid)
    if ai == nil {
        return
    }

    var targets = make([]nnet.SessionID, 0)
    adapterLock.Lock()
    for k := range GlobalGateways {
        targets = append(targets, k)
    }
    adapterLock.Unlock()

    msg := messages.CreateS2SMessage(messages.ProtocolTypeAdapterChange)

    a := make(codecs.IMMap)
    a[messages.ProtocolKeySessionId] = sessionid
    a[messages.ProtocolKeyId] = ai.pid
    a[messages.ProtocolKeyUnixAddr] = ai.unixAddr
    a[messages.ProtocolKeyHost] = ai.host
    a[messages.ProtocolKeyUnixMsgAddr] = ai.unixMsgAddr
    a[messages.ProtocolKeyValue] = ai.connection

    msg.SetTag(messages.ProtocolTagClient)
    msg.SetBody(a)

    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        for _, v := range targets {
            tcp.Send(v, pck)
        }
    }
}

func notifySlaveBye(sessionid nnet.SessionID) {
    var targets = make([]nnet.SessionID, 0)
    adapterLock.Lock()
    for k := range GlobalAdapters {
        targets = append(targets, k)
    }
    adapterLock.Unlock()

    msg := messages.CreateS2SMessage(messages.ProtocolTypeSlaveBye)

    a := make(codecs.IMMap)
    a[messages.ProtocolKeySessionId] = sessionid
    msg.SetTag(messages.ProtocolTagAdapter)
    msg.SetBody(a)

    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        for _, v := range targets {
            tcp.Send(v, pck)
        }
    }
}

func notifyAdapterBye(sessionid nnet.SessionID) {

    var targets = make([]nnet.SessionID, 0)
    adapterLock.Lock()
    for k := range GlobalGateways {
        targets = append(targets, k)
    }
    adapterLock.Unlock()

    msg := messages.CreateS2SMessage(messages.ProtocolTypeAdapterBye)

    a := make(codecs.IMMap)
    a[messages.ProtocolKeySessionId] = sessionid
    msg.SetTag(messages.ProtocolTagClient)
    msg.SetBody(a)

    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        for _, v := range targets {
            tcp.Send(v, pck)
        }
    }
}

func OnSlaveSayHello(message *messages.Message) error {
    body := message.GetBody()
    reader := codecs.CreateMapReader(body)
    pid := int(reader.IntValueOf(messages.ProtocolKeyId, 0))
    vmFree := int(reader.IntValueOf(messages.ProtocolKeyValue, 0))
    unixAddr := reader.StrValueOf(messages.ProtocolKeyUnixAddr, "")

    utils.LogInfo(">>> Slave %s 上线.", message.GetSource())
    si := SlaveInfo{pid: pid, host: strings.Split(message.GetSource(), ":")[0], vmFree: vmFree, unixAddr: unixAddr}
    addSlave(message.GetController().GetSessionID(), si)

    message.GetController().SetTag(messages.ProtocolTagSlave)

    notifySlaveCome(message.GetController().GetSessionID(), si)
    /*
    msg := messages.CreateS2SMessage(messages.ProtocolTypeAdapters)
    msg.SetTag(messages.ProtocolTagSlave)
    req := codecs.IMMap{}
    req[messages.ProtocolKeyValue] = getAdapters()
    msg.SetBody(req)
    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        message.GetController().Send(pck)
    }*/
    return nil
}

func OnSlaveChange(message *messages.Message) error {
    body := message.GetBody()
    reader := codecs.CreateMapReader(body)
    //pid := int(reader.IntValueOf(messages.ProtocolKeyId, 0))
    vmFree := int(reader.IntValueOf(messages.ProtocolKeyValue, 0))
    //unixAddr := reader.StrValueOf(messages.ProtocolKeyUnixAddr, "")

    //utils.LogInfo(">>> Slave %s 上报.", message.GetSource())
    //si := SlaveInfo{pid: pid, host: strings.Split(message.GetSource(), ":")[0], vmFree: vmFree, unixAddr: unixAddr}
    //addSlave(message.GetController().GetSessionID(), si)
    updateSlave(message.GetController().GetSessionID(), vmFree)

    message.GetController().SetTag(messages.ProtocolTagSlave)

    notifySlaveChange(message.GetController().GetSessionID())
    return nil
}

func OnAdapterSayHello(message *messages.Message) error {
    body := message.GetBody()
    reader := codecs.CreateMapReader(body)
    pid := int(reader.IntValueOf(messages.ProtocolKeyId, 0))
    connection := int(reader.IntValueOf(messages.ProtocolKeyValue, 0))
    unixAddr := reader.StrValueOf(messages.ProtocolKeyUnixAddr, "")
    unixMsgAddr := reader.StrValueOf(messages.ProtocolKeyUnixMsgAddr, "")

    utils.LogInfo(">>> Adapter %s 上线.", message.GetSource())
    ai := AdapterInfo{pid: pid, host: strings.Split(message.GetSource(), ":")[0], connection: connection, unixAddr: unixAddr, unixMsgAddr: unixMsgAddr}
    addAdapter(message.GetController().GetSessionID(), ai)

    message.GetController().SetTag(messages.ProtocolTagAdapter)

    notifyAdapterCome(message.GetController().GetSessionID(), ai)

    msg := messages.CreateS2SMessage(messages.ProtocolTypeSlaves)
    msg.SetTag(messages.ProtocolTagAdapter)
    req := codecs.IMMap{}
    req[messages.ProtocolKeyLocalHost] = ai.host
    req[messages.ProtocolKeyValue] = getSlaves()
    msg.SetBody(req)
    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        message.GetController().Send(pck)
    }

    return nil
}

func OnAdapterChange(message *messages.Message) error {
    body := message.GetBody()
    reader := codecs.CreateMapReader(body)
    //pid := int(reader.IntValueOf(messages.ProtocolKeyId, 0))
    connection := int(reader.IntValueOf(messages.ProtocolKeyValue, 0))
    //unixAddr := reader.StrValueOf(messages.ProtocolKeyUnixAddr, "")
    //unixMsgAddr := reader.StrValueOf(messages.ProtocolKeyUnixMsgAddr, "")

    //ai := AdapterInfo{pid: pid, host: strings.Split(message.GetSource(), ":")[0], connection: connection, unixAddr: unixAddr, unixMsgAddr: unixMsgAddr}
    updateAdapter(message.GetController().GetSessionID(), connection)

    message.GetController().SetTag(messages.ProtocolTagAdapter)

    notifyAdapterChange(message.GetController().GetSessionID())

    return nil
}

func OnGatewaySayHello(message *messages.Message) error {
    body := message.GetBody()
    reader := codecs.CreateMapReader(body)
    pid := int(reader.IntValueOf(messages.ProtocolKeyId, 0))
    addr := reader.StrValueOf(messages.ProtocolKeyValue, "")
    gi := GatewayInfo{pid: pid, host: strings.Split(message.GetSource(), ":")[0], addr: addr}

    utils.LogInfo(">>> Gateway %s 上线.", message.GetSource())
    addGateway(message.GetController().GetSessionID(), gi)

    message.GetController().SetTag(messages.ProtocolTagClient)

    msg := messages.CreateS2SMessage(messages.ProtocolTypeAdapters)
    msg.SetTag(messages.ProtocolTagClient)
    req := codecs.IMMap{}
    req[messages.ProtocolKeyLocalHost] = gi.host
    req[messages.ProtocolKeyValue] = getAdapters()
    msg.SetBody(req)
    pck, err := messages.DataFromMessage(msg)
    if err == nil {
        message.GetController().Send(pck)
    }
    return nil
}

type MasterMessageObject struct {
}

func (receiver MasterMessageObject) GetMappedTypes() (map[int]messages.MessageProcFunc) {
    msgMap := make(map[int]messages.MessageProcFunc)
    msgMap[messages.ProtocolTypeSlaveHello] = OnSlaveSayHello
    msgMap[messages.ProtocolTypeAdapterHello] = OnAdapterSayHello
    msgMap[messages.ProtocolTypeGatewayHello] = OnGatewaySayHello
    msgMap[messages.ProtocolTypeSlaveChange] = OnSlaveChange
    msgMap[messages.ProtocolTypeAdapterChange] = OnAdapterChange

    return msgMap
}

func OnSlaveDeliver(message *messages.Message) error {
    target := message.GetSessionId()[0]
    message.SetSessionId([]nnet.SessionID{message.GetController().GetSessionID()})
    pck, err := messages.DataFromMessage(message)
    if err == nil {
        tcp.Send(target, pck)
    }
    return nil
}
type SlaveMessageObject struct {
}

func (receiver SlaveMessageObject) GetMappedTypes() (map[int]messages.MessageProcFunc) {
    msgMap := make(map[int]messages.MessageProcFunc)
    msgMap[messages.ProtocolTypeDeliver] = OnSlaveDeliver
    return msgMap
}

func OnAdapterDeliver(message *messages.Message) error {
    var target nnet.SessionID = 0
    sess := message.GetSessionId()
    if sess != nil && len(sess) > 0 {
        target = sess[0]
    }
    message.SetSessionId([]nnet.SessionID{message.GetController().GetSessionID()})
    pck, err := messages.DataFromMessage(message)
    if err == nil {
        if target != 0 {
            tcp.Send(target, pck)
        } else {
            eachAdapters(func(k nnet.SessionID, v AdapterInfo) {
                tcp.Send(k, pck)
            })
        }
    }
    return nil
}
type AdapterMessageObject struct {
}

func (receiver AdapterMessageObject) GetMappedTypes() (map[int]messages.MessageProcFunc) {
    msgMap := make(map[int]messages.MessageProcFunc)
    msgMap[messages.ProtocolTypeDeliver] = OnAdapterDeliver
    return msgMap
}