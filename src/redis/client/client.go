package client

import (
    "bufio"
    "context"
    "errors"
    "github.com/HDT3213/godis/src/interface/redis"
    "github.com/HDT3213/godis/src/lib/logger"
    "github.com/HDT3213/godis/src/lib/sync/wait"
    "github.com/HDT3213/godis/src/redis/reply"
    "io"
    "net"
    "strconv"
    "strings"
    "sync"
    "time"
)

type Client struct {
    conn        net.Conn //与服务端的tcp连接
    sendingReqs chan *Request // 等待发送的请求
    waitingReqs chan *Request // 等待服务器响应的请求
    ticker      *time.Ticker // 用于触发心跳包的计时器
    addr        string

    ctx        context.Context // 优雅关闭处理
    cancelFunc context.CancelFunc // 优雅关闭处理
    writing    *sync.WaitGroup // 有请求正在处理不能立即停止，用于实现优雅关闭
}

type Request struct {
    id        uint64 // 请求id
    args      [][]byte // 参数
    reply     redis.Reply // 收到的返回值
    heartbeat bool // 标记是否是心跳请求
    waiting   *wait.Wait // 通过协程发送请求后通过waitGroup等待异步处理完成
    err       error
}

const (
    chanSize = 256
    maxWait  = 3 * time.Second
)

func MakeClient(addr string) (*Client, error) {
    //建立客户端连接
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return nil, err
    }
    ctx, cancel := context.WithCancel(context.Background())
    return &Client{
        addr:        addr,
        conn:        conn,
        sendingReqs: make(chan *Request, chanSize),
        waitingReqs: make(chan *Request, chanSize),
        ctx:         ctx,
        cancelFunc:  cancel,
        writing:     &sync.WaitGroup{},
    }, nil
}

func (client *Client) Start() {
    client.ticker = time.NewTicker(10 * time.Second)
    go client.handleWrite()
    go func() {
        err := client.handleRead()
        logger.Warn(err)
    }()
    go client.heartbeat()
}

func (client *Client) Close() {
    // 阻止新请求进入队列
    close(client.sendingReqs)

    // 等待处理中的请求处理完毕
    client.writing.Wait()

    // 释放资源
    client.cancelFunc()
    _ = client.conn.Close()
    close(client.waitingReqs)
}

func (client *Client) handleConnectionError(err error) error {
    err1 := client.conn.Close()
    if err1 != nil {
        if opErr, ok := err1.(*net.OpError); ok {
            if opErr.Err.Error() != "use of closed network connection" {
                return err1
            }
        } else {
            return err1
        }
    }
    conn, err1 := net.Dial("tcp", client.addr)
    if err1 != nil {
        logger.Error(err1)
        return err1
    }
    client.conn = conn
    go func() {
        _ = client.handleRead()
    }()
    return nil
}

func (client *Client) heartbeat() {
loop:
    for {
        select {
        case <-client.ticker.C:
            client.sendingReqs <- &Request{
                args:      [][]byte{[]byte("PING")},
                heartbeat: true,
            }
        case <-client.ctx.Done():
            break loop
        }
    }
}

func (client *Client) handleWrite() {
loop:
    for {
        select {
        case req := <-client.sendingReqs:
            //增加未完成请求
            client.writing.Add(1)
            //发送请求
            client.doRequest(req)
        case <-client.ctx.Done():
            break loop
        }
    }
}

// todo: wait with timeout
func (client *Client) Send(args [][]byte) redis.Reply {
    request := &Request{
        args:      args,
        heartbeat: false,
        waiting:   &wait.Wait{},
    }
    request.waiting.Add(1)
    client.sendingReqs <- request // 将请求发往处理队列
    timeout := request.waiting.WaitWithTimeout(maxWait) // 等待请求处理完成或者超时
    if timeout {
        return reply.MakeErrReply("server time out")
    }
    if request.err != nil {
        return reply.MakeErrReply("request failed")
    }
    return request.reply
}

func (client *Client) doRequest(req *Request) {
    //序列化
    bytes := reply.MakeMultiBulkReply(req.args).ToBytes()
    //tcp connection
    _, err := client.conn.Write(bytes)
    i := 0
    //重试机制
    for err != nil && i < 3 {
        //断开当前连接 重新建立新连接
        err = client.handleConnectionError(err)
        if err == nil {
            _, err = client.conn.Write(bytes)
        }
        i++
    }
    if err == nil {
        //将发送成功请求放入等待响应队列
        client.waitingReqs <- req
    } else {
        req.err = err
        //结束调用者等待
        req.waiting.Done()
        //结束该请求
        client.writing.Done()
    }
}

func (client *Client) finishRequest(reply redis.Reply) {
    //取出等待响应的req
    request := <-client.waitingReqs
    request.reply = reply
    if request.waiting != nil {
        //结束调用者的等待
        request.waiting.Done()
    }
    //减小完成的请求数
    client.writing.Done()
}

// Redis协议解析器 RESP
func (client *Client) handleRead() error {
    reader := bufio.NewReader(client.conn)
    downloading := false
    expectedArgsCount := 0
    receivedCount := 0
    msgType := byte(0) // first char of msg
    var args [][]byte
    var fixedLen int64 = 0
    var err error
    var msg []byte
    for {
        // read line
        if fixedLen == 0 { // read normal line
            msg, err = reader.ReadBytes('\n')
            if err != nil {
                if err == io.EOF || err == io.ErrUnexpectedEOF {
                    logger.Info("connection close")
                } else {
                    logger.Warn(err)
                }

                return errors.New("connection closed")
            }
            if len(msg) == 0 || msg[len(msg)-2] != '\r' {
                return errors.New("protocol error")
            }
        } else { // read bulk line (binary safe)
            msg = make([]byte, fixedLen+2)
            _, err = io.ReadFull(reader, msg)
            if err != nil {
                if err == io.EOF || err == io.ErrUnexpectedEOF {
                    return errors.New("connection closed")
                } else {
                    return err
                }
            }
            if len(msg) == 0 ||
                msg[len(msg)-2] != '\r' ||
                msg[len(msg)-1] != '\n' {
                return errors.New("protocol error")
            }
            fixedLen = 0
        }

        // parse line
        if !downloading {
            // receive new response
            if msg[0] == '*' { // multi bulk response
                // bulk multi msg
                expectedLine, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
                if err != nil {
                    return errors.New("protocol error: " + err.Error())
                }
                if expectedLine == 0 {
                    client.finishRequest(&reply.EmptyMultiBulkReply{})
                } else if expectedLine > 0 {
                    msgType = msg[0]
                    downloading = true
                    expectedArgsCount = int(expectedLine)
                    receivedCount = 0
                    args = make([][]byte, expectedLine)
                } else {
                    return errors.New("protocol error")
                }
            } else if msg[0] == '$' { // bulk response
                fixedLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
                if err != nil {
                    return err
                }
                if fixedLen == -1 { // null bulk
                    client.finishRequest(&reply.NullBulkReply{})
                    fixedLen = 0
                } else if fixedLen > 0 {
                    msgType = msg[0]
                    downloading = true
                    expectedArgsCount = 1
                    receivedCount = 0
                    args = make([][]byte, 1)
                } else {
                    return errors.New("protocol error")
                }
            } else { // single line response
                str := strings.TrimSuffix(string(msg), "\n")
                str = strings.TrimSuffix(str, "\r")
                var result redis.Reply
                switch msg[0] {
                case '+':
                    result = reply.MakeStatusReply(str[1:])
                case '-':
                    result = reply.MakeErrReply(str[1:])
                case ':':
                    val, err := strconv.ParseInt(str[1:], 10, 64)
                    if err != nil {
                        return errors.New("protocol error")
                    }
                    result = reply.MakeIntReply(val)
                }
                client.finishRequest(result)
            }
        } else {
            // receive following part of a request
            line := msg[0 : len(msg)-2]
            if line[0] == '$' {
                fixedLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
                if err != nil {
                    return err
                }
                if fixedLen <= 0 { // null bulk in multi bulks
                    args[receivedCount] = []byte{}
                    receivedCount++
                    fixedLen = 0
                }
            } else {
                args[receivedCount] = line
                receivedCount++
            }

            // if sending finished
            if receivedCount == expectedArgsCount {
                downloading = false // finish downloading progress

                if msgType == '*' {
                    reply := reply.MakeMultiBulkReply(args)
                    client.finishRequest(reply)
                } else if msgType == '$' {
                    reply := reply.MakeBulkReply(args[0])
                    client.finishRequest(reply)
                }


                // finish reply
                expectedArgsCount = 0
                receivedCount = 0
                args = nil
                msgType = byte(0)
            }
        }
    }
}
