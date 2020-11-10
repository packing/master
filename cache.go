package main

import (
    "sync"

    "github.com/packing/nbpy/codecs"
    "github.com/packing/nbpy/messages"
    "github.com/packing/nbpy/nnet"
)

type SlaveInfo struct {
    pid      int
    host     string
    vmFree   int
    unixAddr string
}

type AdapterInfo struct {
    pid         int
    host        string
    connection  int
    unixAddr    string
    unixMsgAddr string
}

type GatewayInfo struct {
    pid  int
    host string
    addr string
}

var (
    GlobalSlaves   = make(map[nnet.SessionID]SlaveInfo)
    GlobalAdapters = make(map[nnet.SessionID]AdapterInfo)
    GlobalGateways = make(map[nnet.SessionID]GatewayInfo)

    adapterLock sync.Mutex
    slaveLock   sync.Mutex
    gatewayLock sync.Mutex
)

func addAdapter(adapterId nnet.SessionID, ai AdapterInfo) {
    adapterLock.Lock()
    defer adapterLock.Unlock()
    GlobalAdapters[adapterId] = ai
}

func getAdapters() codecs.IMSlice {
    adapterLock.Lock()
    defer adapterLock.Unlock()
    slice := make(codecs.IMSlice, len(GlobalAdapters))
    i := 0
    for k, v := range GlobalAdapters {
        a := make(codecs.IMMap)
        a[messages.ProtocolKeySessionId] = k
        a[messages.ProtocolKeyId] = v.pid
        a[messages.ProtocolKeyUnixAddr] = v.unixAddr
        a[messages.ProtocolKeyHost] = v.host
        a[messages.ProtocolKeyUnixMsgAddr] = v.unixMsgAddr
        a[messages.ProtocolKeyValue] = v.connection
        slice[i] = a
    }
    return slice
}

func eachAdapters(fn func(k nnet.SessionID, v AdapterInfo)) {
    adapterLock.Lock()
    defer adapterLock.Unlock()
    for k, v := range GlobalAdapters {
        fn(k, v)
    }
}

func updateAdapter(adapterId nnet.SessionID, connection int) {
    adapterLock.Lock()
    defer adapterLock.Unlock()
    ai, ok := GlobalAdapters[adapterId]
    if ok {
        ai.connection = connection
        GlobalAdapters[adapterId] = ai
    }
}

func getAdapter(adapterId nnet.SessionID) *AdapterInfo {
    adapterLock.Lock()
    defer adapterLock.Unlock()
    ai, ok := GlobalAdapters[adapterId]
    if ok {
        return &ai
    }
    return nil
}

func delAdapter(adapterId nnet.SessionID) {
    adapterLock.Lock()
    defer adapterLock.Unlock()
    delete(GlobalAdapters, adapterId)
}

func addSlave(slaveId nnet.SessionID, si SlaveInfo) {
    slaveLock.Lock()
    defer slaveLock.Unlock()
    GlobalSlaves[slaveId] = si
}

func getSlaves() codecs.IMSlice {
    slaveLock.Lock()
    defer slaveLock.Unlock()
    slice := make(codecs.IMSlice, len(GlobalSlaves))
    i := 0
    for k, v := range GlobalSlaves {
        a := make(codecs.IMMap)
        a[messages.ProtocolKeySessionId] = k
        a[messages.ProtocolKeyId] = v.pid
        a[messages.ProtocolKeyUnixAddr] = v.unixAddr
        a[messages.ProtocolKeyHost] = v.host
        a[messages.ProtocolKeyValue] = v.vmFree
        slice[i] = a
    }
    return slice
}

func updateSlave(slaveId nnet.SessionID, vmFree int) {
    slaveLock.Lock()
    defer slaveLock.Unlock()
    si, ok := GlobalSlaves[slaveId]
    if ok {
        si.vmFree = vmFree
        GlobalSlaves[slaveId] = si
    }
}

func getSlave(slaveId nnet.SessionID) *SlaveInfo {
    slaveLock.Lock()
    defer slaveLock.Unlock()
    si, ok := GlobalSlaves[slaveId]
    if ok {
        return &si
    }
    return nil
}

func delSlave(slaveId nnet.SessionID) {
    slaveLock.Lock()
    defer slaveLock.Unlock()
    delete(GlobalSlaves, slaveId)
}

func addGateway(gatewayId nnet.SessionID, gi GatewayInfo) {
    gatewayLock.Lock()
    defer gatewayLock.Unlock()
    GlobalGateways[gatewayId] = gi
}

func delGateway(gatewayId nnet.SessionID) {
    gatewayLock.Lock()
    defer gatewayLock.Unlock()
    delete(GlobalGateways, gatewayId)
}
