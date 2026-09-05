// 设计依据：docs/dev/一期实现内容.md §6「出口标准」

package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// echoModel is the fallback used when a version's API key cannot be resolved.
//
// Its purpose is to keep the pipeline verifiable without a provider account:
// Gateway, queue, lease, assembly, event persistence and outbound delivery all
// execute for real, and only the text is canned. Swapping in the real provider
// is a configuration change, not a code change.
//
// It is deliberately obvious in its output. A silent stand-in that looked like
// a real answer would be far worse than no answer at all — someone would
// eventually ship it believing the model was wired up.
type echoModel struct {
	name   string
	reason string
}

var _ model.Model = (*echoModel)(nil)

func newEchoModel(name, reason string) *echoModel {
	return &echoModel{name: name, reason: reason}
}

// Info identifies the stand-in, so logs and traces show what actually ran.
func (m *echoModel) Info() model.Info {
	return model.Info{Name: m.name + " (stub)"}
}

// GenerateContent returns one non-streaming response echoing the last user
// message.
func (m *echoModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	ch := make(chan *model.Response, 1)

	var lastUser string
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == model.RoleUser {
			lastUser = req.Messages[i].Content
			break
		}
	}

	var b strings.Builder
	b.WriteString("[模型未接入] ")
	b.WriteString(m.reason)
	if lastUser != "" {
		b.WriteString("\n收到你的消息：")
		b.WriteString(lastUser)
	}

	ch <- &model.Response{
		ID:      "stub-" + m.name,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   m.Info().Name,
		Choices: []model.Choice{{
			Index:        0,
			Message:      model.NewAssistantMessage(b.String()),
			FinishReason: strPtr("stop"),
		}},
		Usage:     &model.Usage{},
		Done:      true,
		Timestamp: time.Now(),
	}
	close(ch)
	return ch, nil
}

func strPtr(s string) *string { return &s }

// describeMissingKey explains why the stand-in was used, so the reason reaches
// the user rather than only the log.
func describeMissingKey(ref string, err error) string {
	if ref == "" {
		return "该 Agent 版本未配置模型密钥引用。"
	}
	return fmt.Sprintf("密钥 %s 未能解析（%v），已回退到占位模型。", ref, err)
}
