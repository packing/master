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
    GlobalSlaves   = new(sync.Map)
    GlobalAdapters = new(sync.Map)
    GlobalGateways = new(sync.Map)
)

func addAdapter(adapterId nnet.SessionID, ai AdapterInfo) {
    GlobalAdapters.Store(adapterId, ai)
}

func getAdapters() codecs.IMSlice {
    slice := make(codecs.IMSlice, 0)
    GlobalAdapters.Range(func(key, value interface{}) bool {
        v, ok := value.(AdapterInfo)
        if !ok {
            return false
        }
        a := make(codecs.IMMap)
        a[messages.ProtocolKeySessionId] = key
        a[messages.ProtocolKeyId] = v.pid
        a[messages.ProtocolKeyUnixAddr] = v.unixAddr
        a[messages.ProtocolKeyHost] = v.host
        a[messages.ProtocolKeyUnixMsgAddr] = v.unixMsgAddr
        a[messages.ProtocolKeyValue] = v.connection
        slice = append(slice, a)
        return true
    })
    return slice
}

func eachAdapters(fn func(k nnet.SessionID, v AdapterInfo)) {
    GlobalAdapters.Range(func(key, value interface{}) bool {
        k, ok := key.(nnet.SessionID)
        if !ok {
            return false
        }
        v, ok := value.(AdapterInfo)
        if !ok {
            return false
        }
        fn(k, v)
        return true
    })
}

func updateAdapter(adapterId nnet.SessionID, connection int) {
    value, ok := GlobalAdapters.Load(adapterId)
    if ok {
        v, ok := value.(AdapterInfo)
        if !ok {
            return
        }
        v.connection = connection
        GlobalAdapters.Store(adapterId, v)
    }
}

func getAdapter(adapterId nnet.SessionID) *AdapterInfo {
    value, ok := GlobalAdapters.Load(adapterId)
    if ok {
        v, ok := value.(AdapterInfo)
        if ok {
            return &v
        }
    }
    return nil
}

func delAdapter(adapterId nnet.SessionID) {
    GlobalAdapters.Delete(adapterId)
}

func addSlave(slaveId nnet.SessionID, si SlaveInfo) {
    GlobalSlaves.Store(slaveId, si)
}

func getSlaves() codecs.IMSlice {
    slice := make(codecs.IMSlice, 0)
    GlobalSlaves.Range(func(key, value interface{}) bool {
        v, ok := value.(SlaveInfo)
        if !ok {
            return false
        }
        a := make(codecs.IMMap)
        a[messages.ProtocolKeySessionId] = key
        a[messages.ProtocolKeyId] = v.pid
        a[messages.ProtocolKeyUnixAddr] = v.unixAddr
        a[messages.ProtocolKeyHost] = v.host
        a[messages.ProtocolKeyValue] = v.vmFree
        slice = append(slice, a)
        return true
    })
    return slice
}

func updateSlave(slaveId nnet.SessionID, vmFree int) {
    value, ok := GlobalSlaves.Load(slaveId)
    if ok {
        v, ok := value.(SlaveInfo)
        if !ok {
            return
        }
        v.vmFree = vmFree
        GlobalSlaves.Store(slaveId, v)
    }
}

func getSlave(slaveId nnet.SessionID) *SlaveInfo {
    value, ok := GlobalSlaves.Load(slaveId)
    if ok {
        v, ok := value.(SlaveInfo)
        if ok {
            return &v
        }
    }
    return nil
}

func delSlave(slaveId nnet.SessionID) {
    GlobalSlaves.Delete(slaveId)
}

func eachSlaves(fn func(k nnet.SessionID, v SlaveInfo)) {
    GlobalSlaves.Range(func(key, value interface{}) bool {
        k, ok := key.(nnet.SessionID)
        if !ok {
            return false
        }
        v, ok := value.(SlaveInfo)
        if !ok {
            return false
        }
        fn(k, v)
        return true
    })
}

func addGateway(gatewayId nnet.SessionID, gi GatewayInfo) {
    GlobalGateways.Store(gatewayId, gi)
}

func delGateway(gatewayId nnet.SessionID) {
    GlobalGateways.Delete(gatewayId)
}

func eachGateways(fn func(k nnet.SessionID, v GatewayInfo)) {
    GlobalGateways.Range(func(key, value interface{}) bool {
        k, ok := key.(nnet.SessionID)
        if !ok {
            return false
        }
        v, ok := value.(GatewayInfo)
        if !ok {
            return false
        }
        fn(k, v)
        return true
    })
}
