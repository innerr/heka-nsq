package nsq

import (
	"bytes"
	"errors"
	"net/http"
	"strconv"
	"io/ioutil"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
)

type NSQOutputConfig struct {
	Address string
	RoutingKey string
}

type NSQOutput struct {
	config *NSQOutputConfig
}

func (ao *NSQOutput) ConfigStruct() interface{} {
	return &NSQOutputConfig{}
}

func (ao *NSQOutput) Init(config interface{}) (err error) {
	ao.config = config.(*NSQOutputConfig)
	return
}

func (ao *NSQOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	inChan := or.InChan()

	var pack *PipelinePack
	var msg *message.Message
	var body []byte
	var respData []byte
	var resp *http.Response

	ok := true
	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				break
			}
			msg = pack.Message
			body = []byte(msg.GetPayload())
			resp, err = http.Post("http://", "application/octet-stream", bytes.NewReader(body))
			if err != nil {
				or.LogError(err)
				break
			}
			if resp.StatusCode != 200 {
				or.LogError(errors.New("Error code: " + strconv.Itoa(resp.StatusCode)))
				break
			}
			respData, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				or.LogError(err)
				break
			}
			if !bytes.Equal(respData, []byte("OK")) {
				resp.Body.Close()
				or.LogError(errors.New("Error response: " + string(respData)))
				break
			} else {
				pack.Recycle()
			}
			resp.Body.Close()
		}
	}
	return
}

func (ao *NSQOutput) CleanupForRestart() {
}

func init() {
	RegisterPlugin("NSQOutput", func() interface{} {
		return new(NSQOutput)
	})
}
