
package main
import (
	"fmt"
	bs "example/gRPC_4Stream/thrift/gen-go/generic"
	sessionbs "example/gRPC_4Stream/thrift/gen-go/session"
	"time"

	"example/gRPC_4Stream/thriftpool"
	"git.apache.org/thrift.git/lib/go/thrift"
	idbs "example/gRPC_4Stream/thrift/gen-go/idgenerate"
)


//-----------truycap data---------------------
func BigSetClientCreator(host, port string, connTimeout time.Duration, forPool* thriftpool.ThriftPool) (*thriftpool.ThriftSocketClient, error){
	socket, err := thrift.NewTSocketTimeout(fmt.Sprintf("%s:%s", host, port), connTimeout)
	if err != nil {
		return nil, err
	}
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	client := bs.NewTStringBigSetKVServiceClientFactory(transportFactory.GetTransport(socket), protocolFactory)

	err = client.Transport.Open()
	if err != nil {
		return nil, err
	}
	return &thriftpool.ThriftSocketClient{
		Client: client,
		Socket: socket,
		Parent: forPool,
	}, nil
}


var (mp=thriftpool.NewMapPool(100, 3600, 3600, BigSetClientCreator, close))

//Create Session
func SessionClientCreator(host, port string, connTimeout time.Duration, forPool* thriftpool.ThriftPool) (*thriftpool.ThriftSocketClient, error){

	socket, err := thrift.NewTSocketTimeout(fmt.Sprintf("%s:%s", host, port), connTimeout)
	if err != nil {
		return nil, err
	}
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTCompactProtocolFactory()
	ssclient := sessionbs.NewTSimpleSessionService_WClientFactory(transportFactory.GetTransport(socket), protocolFactory)
	err = ssclient.Transport.Open()
	if err != nil {
		return nil, err
	}
	return &thriftpool.ThriftSocketClient{
		Client: ssclient,
		Socket: socket,
		Parent: forPool,
	}, nil
}
var (mpcreatekey=thriftpool.NewMapPool(100, 3600, 3600, SessionClientCreator, close))


//create id

func IdClientCreator(host, port string, connTimeout time.Duration, forPool* thriftpool.ThriftPool) (*thriftpool.ThriftSocketClient, error){

	socket, err := thrift.NewTSocketTimeout(fmt.Sprintf("%s:%s", host, port), connTimeout)
	if err != nil {
		return nil, err
	}

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	//protocolFactory := thrift.NewTCompactProtocolFactory()
	idclient := idbs.NewTGeneratorClientFactory(transportFactory.GetTransport(socket), protocolFactory)
	err = idclient.Transport.Open()
	if err != nil {
		return nil, err
	}
	return &thriftpool.ThriftSocketClient{
		Client: idclient,
		Socket: socket,
		Parent: forPool,
	}, nil
}

var (mpid=thriftpool.NewMapPool(100, 3600, 3600, IdClientCreator, close))

func getvalue(t string) int64{
	idclient, _ := mpid.Get("127.0.0.1", "18405").Get()
	defer idclient.BackToPool()
	id, _ := idclient.Client.(*idbs.TGeneratorClient).GetValue(t)
	return id
}

func close(c *thriftpool.ThriftSocketClient) error {
	err := c.Socket.Close()
	//err = c.Client.(*tutorial.PlusServiceClient).Transport.Close()
	return err
}
