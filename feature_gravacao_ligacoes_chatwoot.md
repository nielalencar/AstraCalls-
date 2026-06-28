# Feature Guide — Gravação de Ligações do AstraCalls integrada ao Chatwoot

## 1. Objetivo

Implementar uma feature para **gravar chamadas de voz realizadas ou recebidas pelo AstraCalls** e anexar automaticamente a gravação na **mesma conversa do contato no Chatwoot**.

A gravação deve aparecer no histórico do atendimento como uma mensagem/nota com anexo, preferencialmente como **nota privada**, para que a equipe consiga ouvir a ligação dentro da conversa sem expor o arquivo ao cliente.

---

## 2. Contexto técnico atual

O AstraCalls já faz a ponte de mídia entre:

```text
Navegador do operador
↓ WebRTC / Opus
Servidor AstraCalls em Go
↓ transcodificação Opus/PCM/MLow
WhatsApp / relay SRTP
↓
Contato remoto
```

Segundo o README do projeto, o AstraCalls envia o microfone do navegador por WebRTC para o servidor Go, transcodifica para o codec MLow da Meta e faz o caminho inverso para devolver o áudio do contato ao navegador. O README também informa que, sem `opus_mlow` via `cgo`, o sistema opera em modo “somente sinalização”, isto é, pareamento e setup de chamada funcionam, mas sem áudio ao vivo.

Referência: https://github.com/AstraOnlineWeb/AstraCalls

No código atual, a API de iniciar chamada já possui um campo `record` no body:

```go
var body struct {
  Phone      string `json:"phone"`
  DurationMs int    `json:"duration_ms"`
  Record     bool   `json:"record"`
}
```

Porém, no fluxo atual, esse valor ainda não é usado para iniciar uma gravação. A chamada é iniciada assim:

```go
callID, err := sess.startOutgoing(r.Context(), peer, false)
```

Ou seja: **existe intenção de API, mas falta a implementação da gravação**.

---

## 3. Resultado esperado

Ao final da implementação, deve ser possível:

1. Iniciar uma chamada com `record: true`.
2. Gravar o áudio dos dois lados da ligação.
3. Finalizar a gravação automaticamente quando a chamada terminar.
4. Salvar o arquivo em disco, volume Docker ou storage externo.
5. Encontrar a conversa correta no Chatwoot.
6. Enviar a gravação como anexo na conversa do contato.
7. Registrar metadados da gravação no histórico da chamada.
8. Permitir retry caso o upload para o Chatwoot falhe.

---

## 4. Escopo funcional

### 4.1. Dentro do escopo

- Gravação de chamadas outbound.
- Gravação de chamadas inbound.
- Gravação por flag `record: true` na API.
- Opção global para gravar todas as chamadas.
- Anexo da gravação na conversa correta do Chatwoot.
- Mensagem privada no Chatwoot com dados da chamada.
- Persistência local em volume Docker.
- Retry de upload para o Chatwoot.
- Registro de status da gravação.

### 4.2. Fora do escopo inicial

- Transcrição automática.
- Resumo com IA.
- Detecção de sentimento.
- Busca por palavras na gravação.
- Interface avançada de gestão de gravações.
- Edição/corte da gravação.

Esses itens podem ser criados como features posteriores.

---

## 5. Decisão de produto

### 5.1. Como a gravação deve aparecer no Chatwoot

Recomendação padrão:

```text
Tipo: mensagem privada / private note
Visível para: agentes e gestores
Não enviada para: cliente/contato
Conteúdo: resumo da chamada
Anexo: arquivo .wav, .ogg ou .mp3
```

Exemplo de mensagem no Chatwoot:

```text
📞 Gravação de chamada WhatsApp

Direção: Realizada
Status: Finalizada
Duração: 02:43
Operador: Miguel
Sessão AstraCalls: comercial-01
Call ID: 605E0C4BF0F053281282FE8AA663AA94

Arquivo anexado automaticamente pelo AstraCalls.
```

### 5.2. Formato inicial recomendado

Para o MVP, usar:

```text
Formato: WAV
Sample rate: 16000 Hz
Canais: 2 canais, se possível
Canal esquerdo: operador
Canal direito: contato
Fallback: mono mixado
```

Motivo: WAV é mais simples para implementar sem depender inicialmente de FFmpeg. Depois, pode-se adicionar conversão assíncrona para `.ogg` ou `.mp3`.

---

## 6. Arquitetura proposta

```text
┌────────────────────────┐
│ Navegador / Operador    │
│ WebRTC / Opus           │
└───────────┬────────────┘
            │ áudio operador
            ▼
┌────────────────────────┐
│ AstraCalls              │
│                         │
│  doWebRTC               │
│  OnBrowserRTP           │──┐
│                         │  │
│  Session.wireCall       │  │
│  OnPeerAudio            │──┤
│                         │  ▼
│  RecordingManager       │
│  CallRecorder           │
│  WAV/OGG Writer         │
└───────────┬────────────┘
            │ arquivo finalizado
            ▼
┌────────────────────────┐
│ Storage local/S3/MinIO  │
└───────────┬────────────┘
            │ upload multipart
            ▼
┌────────────────────────┐
│ Chatwoot API            │
│ Conversation Message    │
│ attachments[]           │
└────────────────────────┘
```

---

## 7. Pontos exatos do código para capturar áudio

### 7.1. Áudio do operador

No arquivo `cmd/server/httpapi.go`, dentro de `doWebRTC`, o áudio do navegador passa por este hook:

```go
bridge.OnBrowserRTP = func(payload []byte) {
  if browserOpus == nil {
    return
  }

  pcm48, err := browserOpus.Decode(payload)
  if err != nil {
    return
  }

  ac.cm.FeedCapturedPCM(media.Downsample48to16(pcm48))
}
```

Aqui é possível capturar o áudio do operador **após decodificar Opus para PCM**.

Implementação proposta:

```go
bridge.OnBrowserRTP = func(payload []byte) {
  if browserOpus == nil {
    return
  }

  pcm48, err := browserOpus.Decode(payload)
  if err != nil {
    return
  }

  pcm16 := media.Downsample48to16(pcm48)

  if ac.recorder != nil {
    ac.recorder.WriteOperatorPCM16(pcm16)
  }

  ac.cm.FeedCapturedPCM(pcm16)
}
```

### 7.2. Áudio do contato remoto

No arquivo `cmd/server/session.go`, dentro de `Session.wireCall`, o áudio vindo do WhatsApp passa por:

```go
cm.OnPeerAudio = func(pcm16 []float32) {
  ac, ok := s.reg.get(callID)
  if !ok || ac.bridge == nil || ac.browserOpus == nil {
    return
  }

  pcm48 := media.Upsample16to48(pcm16)
  opus, err := ac.browserOpus.Encode(pcm48)
  if err != nil || len(opus) == 0 {
    return
  }

  _ = ac.bridge.WriteOpus(opus, 60*time.Millisecond)
}
```

Aqui é possível capturar o áudio do contato **antes de reencodar para o navegador**.

Implementação proposta:

```go
cm.OnPeerAudio = func(pcm16 []float32) {
  ac, ok := s.reg.get(callID)
  if !ok {
    return
  }

  if ac.recorder != nil {
    ac.recorder.WritePeerPCM16(pcm16)
  }

  if ac.bridge == nil || ac.browserOpus == nil {
    return
  }

  pcm48 := media.Upsample16to48(pcm16)
  opus, err := ac.browserOpus.Encode(pcm48)
  if err != nil || len(opus) == 0 {
    return
  }

  _ = ac.bridge.WriteOpus(opus, 60*time.Millisecond)
}
```

---

## 8. Estrutura de dados proposta

### 8.1. Alteração em `activeCall`

Adicionar ao objeto de chamada ativa:

```go
type activeCall struct {
  cm          *call.CallManager
  bridge      *Bridge
  browserOpus media.Codec
  recorder    *CallRecorder
}
```

### 8.2. Novo modelo `CallRecording`

```go
type CallRecording struct {
  ID                    string
  SessionID             string
  CallID                string
  Direction             string
  Peer                  string
  Phone                 string
  SourceID              string
  ChatwootAccountID     int
  ChatwootInboxID       int
  ChatwootContactID     int
  ChatwootConversationID int
  ChatwootMessageID     int
  Status                string
  FilePath              string
  FileName              string
  MimeType              string
  SizeBytes             int64
  DurationMs            int64
  StartedAt             int64
  EndedAt               int64
  Error                 string
  CreatedAt             int64
  UpdatedAt             int64
}
```

### 8.3. Status possíveis

```text
pending
recording
finalizing
saved
uploading
attached
upload_failed
failed
```

---

## 9. Banco de dados

Criar tabela para controlar as gravações:

```sql
CREATE TABLE IF NOT EXISTS call_recordings (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  call_id TEXT NOT NULL,
  direction TEXT,
  peer TEXT,
  phone TEXT,
  source_id TEXT,
  chatwoot_account_id INTEGER,
  chatwoot_inbox_id INTEGER,
  chatwoot_contact_id INTEGER,
  chatwoot_conversation_id INTEGER,
  chatwoot_message_id INTEGER,
  status TEXT NOT NULL DEFAULT 'pending',
  file_path TEXT,
  file_name TEXT,
  mime_type TEXT,
  size_bytes BIGINT DEFAULT 0,
  duration_ms BIGINT DEFAULT 0,
  started_at BIGINT,
  ended_at BIGINT,
  error TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_call_recordings_call_id
  ON call_recordings(call_id);

CREATE INDEX IF NOT EXISTS idx_call_recordings_status
  ON call_recordings(status);

CREATE INDEX IF NOT EXISTS idx_call_recordings_conversation
  ON call_recordings(chatwoot_conversation_id);
```

---

## 10. Configurações novas

Adicionar variáveis de ambiente:

```env
WACALLS_RECORDINGS_ENABLED=true
WACALLS_RECORDINGS_MODE=on_demand
WACALLS_RECORDINGS_DIR=/app/recordings
WACALLS_RECORDINGS_FORMAT=wav
WACALLS_RECORDINGS_PRIVATE_NOTE=true
WACALLS_RECORDINGS_RETENTION_DAYS=90
WACALLS_RECORDINGS_UPLOAD_RETRY=true
WACALLS_RECORDINGS_MAX_FILE_MB=100
```

Modos possíveis:

```text
off         = nunca grava
on_demand   = grava apenas se record=true
always      = grava todas as chamadas
```

---

## 11. Ajuste na stack Docker Swarm

Adicionar volume para gravações:

```yaml
services:
  wacalls:
    image: astraonline/wacalls:develop
    environment:
      - WACALLS_RECORDINGS_ENABLED=true
      - WACALLS_RECORDINGS_MODE=on_demand
      - WACALLS_RECORDINGS_DIR=/app/recordings
      - WACALLS_RECORDINGS_FORMAT=wav
      - WACALLS_RECORDINGS_PRIVATE_NOTE=true
    volumes:
      - wacalls_recordings:/app/recordings

volumes:
  wacalls_recordings:
    name: wacalls_recordings
```

---

## 12. API proposta

### 12.1. Iniciar chamada com gravação

Endpoint atual:

```http
POST /api/sessions/{sid}/calls
```

Body proposto:

```json
{
  "phone": "5551999999999",
  "record": true,
  "chatwoot": {
    "account_id": 1,
    "inbox_id": 10,
    "contact_id": 123,
    "conversation_id": 456,
    "source_id": "astra:comercial-01:5551999999999"
  }
}
```

Resposta:

```json
{
  "call": {
    "callId": "605E0C4BF0F053281282FE8AA663AA94"
  },
  "recording": {
    "enabled": true,
    "status": "recording"
  }
}
```

### 12.2. Iniciar gravação manualmente

```http
POST /api/sessions/{sid}/calls/{callId}/recording/start
```

### 12.3. Parar gravação manualmente

```http
POST /api/sessions/{sid}/calls/{callId}/recording/stop
```

### 12.4. Consultar gravação

```http
GET /api/sessions/{sid}/calls/{callId}/recording
```

Resposta:

```json
{
  "call_id": "605E0C4BF0F053281282FE8AA663AA94",
  "status": "attached",
  "file_name": "call_605E0C4BF0F053281282FE8AA663AA94.wav",
  "duration_ms": 163000,
  "size_bytes": 5293012,
  "chatwoot_conversation_id": 456,
  "chatwoot_message_id": 987
}
```

### 12.5. Reprocessar upload para Chatwoot

```http
POST /api/sessions/{sid}/calls/{callId}/recording/retry-upload
```

---

## 13. Implementação do `CallRecorder`

### 13.1. Interface sugerida

```go
type CallRecorder struct {
  mu            sync.Mutex
  callID        string
  sessionID     string
  filePath      string
  startedAt     time.Time
  sampleRate    int
  channels      int
  writer        *WavWriter
  closed        bool
  operatorQueue chan []float32
  peerQueue     chan []float32
}

func NewCallRecorder(cfg RecordingConfig, meta RecordingMeta) (*CallRecorder, error)
func (r *CallRecorder) Start() error
func (r *CallRecorder) WriteOperatorPCM16(samples []float32)
func (r *CallRecorder) WritePeerPCM16(samples []float32)
func (r *CallRecorder) Stop() (*RecordingResult, error)
func (r *CallRecorder) Abort(reason error)
```

### 13.2. Cuidado com concorrência

Os hooks de áudio podem rodar em goroutines diferentes. Por isso:

- não escrever diretamente no arquivo sem lock;
- preferir canais internos;
- usar buffer limitado;
- se o buffer encher, descartar frame e registrar métrica;
- nunca travar o áudio da ligação por causa da gravação.

Exemplo:

```go
func (r *CallRecorder) WriteOperatorPCM16(samples []float32) {
  if r == nil || r.closed {
    return
  }

  cp := append([]float32(nil), samples...)

  select {
  case r.operatorQueue <- cp:
  default:
    // Evitar travar o áudio da chamada.
    r.dropCounter.Add(1)
  }
}
```

---

## 14. Estratégia de mixagem

### 14.1. Opção A — mono mixado

Mais simples:

```text
final = (operador + contato) / 2
```

Vantagem:

- implementação rápida;
- arquivo menor;
- compatibilidade alta.

Desvantagem:

- não separa quem falou.

### 14.2. Opção B — estéreo separado

Recomendada:

```text
canal esquerdo  = operador
canal direito   = contato
```

Vantagem:

- facilita auditoria;
- facilita transcrição futura com diarização;
- permite equalizar lados separadamente.

Desvantagem:

- exige sincronização mínima entre streams.

### 14.3. Recomendação

Implementar primeiro mono mixado para MVP, mas estruturar `CallRecorder` para aceitar dois canais depois.

---

## 15. Integração com Chatwoot

O Chatwoot permite criar mensagem em uma conversa existente pelo endpoint:

```http
POST /api/v1/accounts/{account_id}/conversations/{conversation_id}/messages
```

A documentação do Chatwoot informa que, para mensagens com anexos, deve ser usado `multipart/form-data` e o arquivo deve ser enviado no campo `attachments[]`.

Referência: https://developers.chatwoot.com/api-reference/messages/create-new-message

### 15.1. Upload da gravação como anexo

Requisição recomendada:

```bash
curl -X POST "https://chatwoot.seudominio.com/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages" \
  -H "api_access_token: ${CHATWOOT_API_TOKEN}" \
  -F "content=📞 Gravação de chamada WhatsApp\n\nDireção: Realizada\nDuração: 02:43\nCall ID: ${CALL_ID}" \
  -F "message_type=outgoing" \
  -F "private=true" \
  -F "content_type=text" \
  -F "attachments[]=@/app/recordings/${FILE_NAME};type=audio/wav"
```

### 15.2. Por que `private=true`

A gravação deve ser uma nota interna por padrão. Isso evita que o cliente receba automaticamente o arquivo da própria chamada.

Se o negócio quiser disponibilizar gravações ao cliente, essa deve ser uma configuração explícita e protegida.

### 15.3. Content type

Para WAV:

```text
audio/wav
```

Para OGG:

```text
audio/ogg
```

Para MP3:

```text
audio/mpeg
```

---

## 16. Como encontrar a conversa correta no Chatwoot

A implementação deve seguir esta ordem:

### 16.1. Caso a chamada tenha sido iniciada pelo widget dentro do Chatwoot

O widget deve enviar no `POST /calls`:

```json
{
  "phone": "5551999999999",
  "record": true,
  "chatwoot": {
    "conversation_id": 456,
    "contact_id": 123,
    "inbox_id": 10,
    "source_id": "astra:comercial-01:5551999999999"
  }
}
```

Nesse caso, o AstraCalls usa diretamente `conversation_id=456`.

### 16.2. Caso a chamada tenha sido feita fora do Chatwoot

O AstraCalls deve:

1. Normalizar telefone.
2. Gerar `source_id` estável.
3. Buscar ou criar contato no Chatwoot.
4. Buscar ou criar conversa do contato.
5. Anexar a gravação na conversa encontrada.

Padrão recomendado:

```text
source_id = astra:{session_id}:{phone_e164_without_plus}
```

Exemplo:

```text
astra:comercial-01:5551999999999
```

---

## 17. Fluxo outbound completo

```text
1. Operador clica para ligar no Chatwoot.
2. Widget chama AstraCalls:
   POST /api/sessions/{sid}/calls { phone, record: true, chatwoot: {...} }
3. AstraCalls inicia chamada.
4. AstraCalls cria CallRecorder se record=true.
5. Áudio do operador é capturado em OnBrowserRTP.
6. Áudio do contato é capturado em OnPeerAudio.
7. Chamada termina.
8. Recorder finaliza arquivo.
9. AstraCalls salva metadados no banco.
10. AstraCalls faz upload para Chatwoot como private note com attachments[].
11. Chatwoot retorna message_id.
12. AstraCalls atualiza call_recordings.status = attached.
```

---

## 18. Fluxo inbound completo

```text
1. Contato liga pelo WhatsApp.
2. AstraCalls recebe CallOffer.
3. Widget/painel toca dentro do Chatwoot.
4. Operador aceita a ligação.
5. AstraCalls identifica contato pelo telefone.
6. AstraCalls encontra ou cria conversa Chatwoot usando source_id estável.
7. Se política permitir, inicia gravação automaticamente.
8. Chamada termina.
9. Recorder finaliza arquivo.
10. AstraCalls anexa gravação na conversa correta.
```

Configuração sugerida para inbound:

```env
WACALLS_RECORDINGS_INBOUND_DEFAULT=true
WACALLS_RECORDINGS_OUTBOUND_DEFAULT=false
```

Ou manter tudo sob `record=true` no MVP.

---

## 19. Retry e tolerância a falhas

### 19.1. Upload falhou

Se o upload para o Chatwoot falhar:

```text
status = upload_failed
error = mensagem do erro
file_path = mantido
```

Criar job de retry:

```text
A cada 5 minutos, buscar gravações upload_failed com menos de 10 tentativas.
```

Tabela auxiliar:

```sql
ALTER TABLE call_recordings
ADD COLUMN upload_attempts INTEGER DEFAULT 0;

ALTER TABLE call_recordings
ADD COLUMN last_upload_attempt_at BIGINT;
```

### 19.2. Chatwoot indisponível

Manter arquivo local e tentar depois. A chamada não deve falhar por causa do Chatwoot.

### 19.3. Erro no recorder

A ligação não deve cair por erro de gravação. O sistema deve registrar falha e continuar a chamada.

---

## 20. Segurança, LGPD e governança

Gravação de chamada é dado sensível do ponto de vista operacional e jurídico. Implementar:

1. Controle de acesso aos arquivos.
2. Links não públicos.
3. Retenção configurável.
4. Exclusão automática após prazo.
5. Registro de quem acessou, se possível.
6. Aviso de gravação quando aplicável.
7. Não expor gravações via URL pública sem autenticação.
8. Separar storage por tenant, se o produto for SaaS.

Recomendação inicial:

```text
Arquivos ficam apenas no volume do AstraCalls.
Chatwoot recebe uma cópia como anexo privado.
Nenhum endpoint público de download sem autenticação.
```

---

## 21. Observabilidade

Adicionar logs estruturados:

```text
recording_started
recording_frame_dropped
recording_finalized
recording_upload_started
recording_upload_succeeded
recording_upload_failed
recording_retry_succeeded
recording_retry_failed
```

Métricas úteis:

```text
recordings_total
recordings_failed_total
recording_upload_failed_total
recording_duration_seconds
recording_file_size_bytes
recording_frames_dropped_total
```

---

## 22. Critérios de aceite

A feature estará pronta quando:

- [ ] `record: true` inicia gravação em chamada outbound.
- [ ] Chamada inbound pode ser gravada por configuração.
- [ ] Arquivo é salvo em `/app/recordings`.
- [ ] Gravação contém áudio dos dois lados.
- [ ] Fim da chamada finaliza corretamente o arquivo.
- [ ] Gravação aparece na conversa correta do Chatwoot.
- [ ] Mensagem no Chatwoot é privada por padrão.
- [ ] Se o upload falhar, gravação fica em `upload_failed` e pode ser reenviada.
- [ ] A chamada não cai se a gravação falhar.
- [ ] Existe endpoint para consultar status da gravação.
- [ ] Logs indicam início, finalização e upload.

---

## 23. Plano de implementação sugerido

### Etapa 1 — MVP local

- Criar `CallRecorder`.
- Salvar WAV mono local.
- Ligar `record: true` ao início do recorder.
- Finalizar arquivo no fim da chamada.

### Etapa 2 — Integração com Chatwoot

- Criar client HTTP para Chatwoot.
- Receber `conversation_id` no contexto da chamada.
- Fazer upload multipart com `attachments[]`.
- Criar mensagem privada.

### Etapa 3 — Persistência e retry

- Criar tabela `call_recordings`.
- Salvar status.
- Implementar job de retry.

### Etapa 4 — Qualidade e segurança

- Retenção automática.
- Limite de tamanho.
- Métricas.
- Logs.
- Testes de carga.

---

## 24. Referências

- AstraCalls: https://github.com/AstraOnlineWeb/AstraCalls
- Chatwoot — Create New Message: https://developers.chatwoot.com/api-reference/messages/create-new-message
- Chatwoot — API Channel Inbox: https://www.chatwoot.com/hc/user-guide/articles/1677839703-how-to-create-an-api-channel-inbox
