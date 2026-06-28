# Feature Guide — Reativação de Ticket Resolvido no Chatwoot usando Source ID do Contato

## 1. Objetivo

Implementar uma feature para que, quando uma conversa/ticket do Chatwoot estiver **resolvida** e o mesmo contato enviar uma nova mensagem posteriormente pelo WhatsApp, o sistema **reabra a mesma conversa** em vez de criar um novo ticket.

A regra central da feature é usar um **`source_id` estável por contato e inbox** como identificador da sessão do contato no Chatwoot.

---

## 2. Contexto técnico

No Chatwoot, uma Inbox representa uma fonte de conversas. Um contato pode ter uma ou mais sessões em uma Inbox por meio de `contact_inboxes`. Cada `contact_inbox` possui um `source_id`, que a documentação descreve como o identificador de sessão usado para criar conversas.

Referência: https://www.chatwoot.com/hc/user-guide/articles/1677839703-how-to-create-an-api-channel-inbox

A criação de uma conversa via Application API exige `source_id`, `inbox_id` e `contact_id`:

```http
POST /api/v1/accounts/{account_id}/conversations
```

Body exemplo:

```json
{
  "source_id": "1234567890",
  "inbox_id": 1,
  "contact_id": 1,
  "status": "open"
}
```

Referência: https://developers.chatwoot.com/api-reference/conversations/create-new-conversation

O Chatwoot também possui endpoint para alterar o status de uma conversa:

```http
POST /api/v1/accounts/{account_id}/conversations/{conversation_id}/toggle_status
```

Body:

```json
{
  "status": "open"
}
```

Referência: https://developers.chatwoot.com/api-reference/conversations/toggle-status

---

## 3. Problema que a feature resolve

Sem uma estratégia correta de `source_id`, integrações WhatsApp → Chatwoot costumam criar conversas duplicadas.

Exemplo do problema:

```text
Dia 1:
Contato 5551999999999 envia mensagem.
Chatwoot cria conversa #100.
Atendente resolve conversa #100.

Dia 2:
Mesmo contato envia nova mensagem.
Integração cria conversa #138.

Resultado:
Histórico fragmentado, perda de contexto e tickets duplicados.
```

Resultado esperado:

```text
Dia 1:
Contato 5551999999999 envia mensagem.
Chatwoot cria conversa #100.
Atendente resolve conversa #100.

Dia 2:
Mesmo contato envia nova mensagem.
Integração identifica source_id estável.
Integração reabre conversa #100.
Nova mensagem entra na conversa #100.
```

---

## 4. Regra central

Nunca gerar `source_id` com valores variáveis, como:

```text
timestamp
message_id
call_id
uuid aleatório
data do atendimento
id da campanha
```

O `source_id` deve ser estável para o par:

```text
sessão AstraCalls / inbox Chatwoot + telefone normalizado
```

---

## 5. Padrão recomendado de Source ID

### 5.1. Para uma operação com apenas uma sessão WhatsApp

```text
source_id = whatsapp:{phone_e164_without_plus}
```

Exemplo:

```text
whatsapp:5551999999999
```

### 5.2. Para operação multi-sessão / multi-QR

Recomendado:

```text
source_id = astra:{session_id}:{phone_e164_without_plus}
```

Exemplo:

```text
astra:comercial-01:5551999999999
```

Motivo: se o mesmo contato falar com dois números diferentes da empresa, cada sessão pode cair em uma Inbox diferente ou operação diferente. O `source_id` precisa evitar colisão entre sessões.

### 5.3. Função de normalização

```go
func NormalizePhone(phone string) string {
  phone = strings.TrimSpace(phone)
  phone = strings.TrimPrefix(phone, "+")

  var b strings.Builder
  for _, c := range phone {
    if c >= '0' && c <= '9' {
      b.WriteRune(c)
    }
  }

  return b.String()
}

func BuildSourceID(sessionID, phone string) string {
  normalized := NormalizePhone(phone)
  return fmt.Sprintf("astra:%s:%s", sessionID, normalized)
}
```

---

## 6. Escopo funcional

### 6.1. Dentro do escopo

- Identificar contato pelo telefone.
- Gerar `source_id` estável.
- Criar ou reutilizar `contact_inbox` com esse `source_id`.
- Buscar conversa existente do contato/inbox/source_id.
- Se a conversa estiver `resolved`, reabrir para `open`.
- Inserir a nova mensagem na mesma `conversation_id`.
- Evitar duplicidade de mensagens por idempotência.
- Registrar mapeamento local entre telefone, source_id e conversation_id.

### 6.2. Fora do escopo inicial

- Criar novo ticket por janela de tempo.
- Separar conversas por campanha.
- Criar múltiplos pipelines dentro do Chatwoot.
- Roteamento avançado por time.
- SLA customizado por tipo de retorno.

Esses itens podem ser adicionados depois como regras configuráveis.

---

## 7. Decisão de produto

Comportamento padrão:

```text
Se o contato já tem conversa naquela inbox, reutilizar sempre a conversa mais recente ligada ao mesmo source_id.
Se essa conversa estiver resolvida, reabrir.
Se não existir conversa, criar nova.
```

Configuração opcional futura:

```env
CHATWOOT_REOPEN_RESOLVED_CONVERSATION=true
CHATWOOT_REOPEN_WINDOW_DAYS=0
```

Interpretação:

```text
0 = reabrir sempre, sem limite de dias
7 = reabrir se última conversa tiver até 7 dias; depois disso cria nova
```

Para a regra solicitada nesta feature, usar:

```text
CHATWOOT_REOPEN_WINDOW_DAYS=0
```

---

## 8. Arquitetura proposta

```text
WhatsApp / AstraCalls
        │
        │ evento message
        ▼
Message Router
        │
        ├── NormalizePhone()
        ├── BuildSourceID()
        ├── FindOrCreateContact()
        ├── FindOrCreateContactInbox()
        ├── FindReusableConversation()
        ├── ReopenIfResolved()
        └── CreateMessageInConversation()
        │
        ▼
Chatwoot Conversation existente
```

---

## 9. Modelo de dados local recomendado

Criar tabela local para não depender apenas de buscas no Chatwoot a cada mensagem.

```sql
CREATE TABLE IF NOT EXISTS chatwoot_contact_links (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  phone TEXT NOT NULL,
  source_id TEXT NOT NULL,
  chatwoot_account_id INTEGER NOT NULL,
  chatwoot_inbox_id INTEGER NOT NULL,
  chatwoot_contact_id INTEGER,
  chatwoot_conversation_id INTEGER,
  last_conversation_status TEXT,
  last_message_at BIGINT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  UNIQUE(session_id, phone, chatwoot_inbox_id)
);

CREATE INDEX IF NOT EXISTS idx_chatwoot_contact_links_source_id
  ON chatwoot_contact_links(source_id);

CREATE INDEX IF NOT EXISTS idx_chatwoot_contact_links_conversation
  ON chatwoot_contact_links(chatwoot_conversation_id);
```

Tabela de idempotência:

```sql
CREATE TABLE IF NOT EXISTS chatwoot_message_dedup (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  source_message_id TEXT NOT NULL,
  phone TEXT NOT NULL,
  chatwoot_conversation_id INTEGER,
  chatwoot_message_id INTEGER,
  created_at BIGINT NOT NULL,
  UNIQUE(session_id, source_message_id)
);
```

---

## 10. Fluxo completo de mensagem recebida

```text
1. AstraCalls recebe evento WhatsApp message.
2. Extrai telefone/JID do contato.
3. Normaliza telefone.
4. Gera source_id estável.
5. Verifica idempotência pelo ID da mensagem WhatsApp.
6. Busca vínculo local em chatwoot_contact_links.
7. Se não existir vínculo:
   7.1. Busca/cria contato no Chatwoot.
   7.2. Cria ou recupera contact_inbox com source_id.
   7.3. Busca conversa existente.
   7.4. Se não existir, cria conversa nova.
   7.5. Salva vínculo local.
8. Se existir vínculo com conversation_id:
   8.1. Consulta status da conversa, ou usa cache confiável.
   8.2. Se status = resolved, chamar toggle_status para open.
9. Cria a nova mensagem incoming na mesma conversation_id.
10. Atualiza last_message_at e last_conversation_status.
11. Marca mensagem como processada na tabela de idempotência.
```

---

## 11. Pseudocódigo principal

```go
func HandleIncomingWhatsAppMessage(ctx context.Context, evt WhatsAppMessageEvent) error {
  phone := NormalizePhone(evt.From)
  sourceID := BuildSourceID(evt.SessionID, phone)

  if dedup.Exists(evt.SessionID, evt.MessageID) {
    return nil
  }

  lockKey := fmt.Sprintf("%s:%s:%d", evt.SessionID, phone, cfg.ChatwootInboxID)
  unlock := locks.Acquire(lockKey)
  defer unlock()

  link, err := repo.FindContactLink(evt.SessionID, phone, cfg.ChatwootInboxID)
  if err != nil {
    return err
  }

  if link == nil {
    contact, err := chatwoot.FindOrCreateContact(ctx, ChatwootContactInput{
      InboxID: cfg.ChatwootInboxID,
      Name: evt.PushName,
      PhoneNumber: "+" + phone,
      SourceID: sourceID,
    })
    if err != nil {
      return err
    }

    conversation, err := chatwoot.FindReusableConversation(ctx, contact.ID, cfg.ChatwootInboxID, sourceID)
    if err != nil {
      return err
    }

    if conversation == nil {
      conversation, err = chatwoot.CreateConversation(ctx, CreateConversationInput{
        SourceID: sourceID,
        InboxID: cfg.ChatwootInboxID,
        ContactID: contact.ID,
        Status: "open",
      })
      if err != nil {
        return err
      }
    }

    link = &ChatwootContactLink{
      SessionID: evt.SessionID,
      Phone: phone,
      SourceID: sourceID,
      AccountID: cfg.ChatwootAccountID,
      InboxID: cfg.ChatwootInboxID,
      ContactID: contact.ID,
      ConversationID: conversation.ID,
      LastConversationStatus: conversation.Status,
    }

    repo.UpsertContactLink(link)
  }

  conversation, err := chatwoot.GetConversation(ctx, link.ConversationID)
  if err != nil {
    return err
  }

  if conversation.Status == "resolved" {
    err = chatwoot.ToggleConversationStatus(ctx, link.ConversationID, "open")
    if err != nil {
      return err
    }
  }

  msg, err := chatwoot.CreateMessage(ctx, CreateMessageInput{
    ConversationID: link.ConversationID,
    Content: evt.Text,
    MessageType: "incoming",
    Private: false,
    ContentType: "text",
  })
  if err != nil {
    return err
  }

  dedup.MarkProcessed(evt.SessionID, evt.MessageID, link.ConversationID, msg.ID)

  link.LastMessageAt = time.Now().UnixMilli()
  link.LastConversationStatus = "open"
  repo.UpsertContactLink(link)

  return nil
}
```

---

## 12. Endpoints Chatwoot usados

### 12.1. Criar conversa

```http
POST /api/v1/accounts/{account_id}/conversations
```

Body:

```json
{
  "source_id": "astra:comercial-01:5551999999999",
  "inbox_id": 10,
  "contact_id": 123,
  "status": "open"
}
```

### 12.2. Buscar detalhes da conversa

```http
GET /api/v1/accounts/{account_id}/conversations/{conversation_id}
```

Usar para confirmar:

```text
status
inbox_id
contact_id
messages
last_activity_at
```

Referência: https://developers.chatwoot.com/api-reference/conversations/conversation-details

### 12.3. Reabrir conversa resolvida

```http
POST /api/v1/accounts/{account_id}/conversations/{conversation_id}/toggle_status
```

Body:

```json
{
  "status": "open"
}
```

### 12.4. Criar mensagem na conversa existente

```http
POST /api/v1/accounts/{account_id}/conversations/{conversation_id}/messages
```

Body:

```json
{
  "content": "Mensagem recebida pelo WhatsApp",
  "message_type": "incoming",
  "private": false,
  "content_type": "text",
  "content_attributes": {}
}
```

Referência: https://developers.chatwoot.com/api-reference/messages/create-new-message

---

## 13. Como buscar a conversa reutilizável

Existem duas estratégias.

### 13.1. Estratégia recomendada: vínculo local

Usar a tabela `chatwoot_contact_links`.

Vantagens:

- rápido;
- menos chamadas à API do Chatwoot;
- reduz risco de duplicidade;
- permite lock local.

Fluxo:

```sql
SELECT *
FROM chatwoot_contact_links
WHERE session_id = $1
  AND phone = $2
  AND chatwoot_inbox_id = $3
LIMIT 1;
```

Se existir `chatwoot_conversation_id`, reutilizar.

### 13.2. Estratégia fallback: buscar conversas do contato

O Chatwoot possui endpoint de conversas de um contato:

```http
GET /api/v1/accounts/{account_id}/contacts/{contact_id}/conversations
```

Usar quando:

- não existe vínculo local;
- vínculo local perdeu referência;
- conversa foi apagada;
- migração inicial.

Referência: https://developers.chatwoot.com/api-reference/contacts/contact-conversations

Regra de seleção:

```text
1. Filtrar por inbox_id.
2. Filtrar por source_id, quando o payload trouxer source_id.
3. Pegar a conversa mais recente.
4. Se status = resolved, reabrir.
5. Se não existir, criar nova.
```

---

## 14. Importante: source_id não deve ser call_id

Errado:

```text
source_id = call_605E0C4BF0F053281282FE8AA663AA94
```

Errado:

```text
source_id = message_3EB0F8A9C921
```

Errado:

```text
source_id = 2026-06-27-5551999999999
```

Certo:

```text
source_id = astra:comercial-01:5551999999999
```

Motivo: o `source_id` precisa representar a sessão/identidade do contato na inbox, não o evento.

---

## 15. Tratamento de ticket resolvido

### 15.1. Regra

Se a conversa encontrada estiver com:

```text
status = resolved
```

Executar:

```http
POST /api/v1/accounts/{account_id}/conversations/{conversation_id}/toggle_status
```

com:

```json
{
  "status": "open"
}
```

Depois criar a mensagem incoming na mesma conversa.

### 15.2. Ordem recomendada

```text
1. Reabrir conversa.
2. Criar mensagem incoming.
```

Motivo: garante que o time veja o ticket como aberto imediatamente e evita mensagem nova entrando em conversa ainda resolvida.

---

## 16. Idempotência

Uma mesma mensagem pode chegar mais de uma vez por retry/webhook. Para evitar duplicidade:

Criar chave única:

```text
session_id + whatsapp_message_id
```

Antes de enviar ao Chatwoot:

```sql
SELECT 1
FROM chatwoot_message_dedup
WHERE session_id = $1
  AND source_message_id = $2;
```

Se existir, ignorar.

Depois que o Chatwoot confirmar criação da mensagem:

```sql
INSERT INTO chatwoot_message_dedup (...)
VALUES (...);
```

---

## 17. Concorrência e race conditions

Problema possível:

```text
O contato envia 2 mensagens quase ao mesmo tempo.
Duas goroutines não encontram conversa local.
As duas criam conversa nova.
```

Solução:

1. Lock por `session_id + phone + inbox_id`.
2. Constraint única no banco.
3. Revalidar depois de adquirir lock.

Exemplo de chave de lock:

```text
chatwoot-link:comercial-01:5551999999999:10
```

---

## 18. Webhooks do Chatwoot para manter estado local

A integração pode receber eventos do Chatwoot como `message_created` e eventos de status, dependendo da configuração de webhooks.

Usos recomendados:

### 18.1. Mensagem outgoing

Quando o agente responde no Chatwoot:

```text
Chatwoot message_created/outgoing
↓
AstraCalls recebe webhook
↓
Envia mensagem para WhatsApp
```

### 18.2. Ticket resolvido

Quando o agente resolve:

```text
Chatwoot conversa alterada para resolved
↓
AstraCalls atualiza chatwoot_contact_links.last_conversation_status = resolved
```

Mesmo que esse webhook não seja implementado no MVP, a feature ainda funciona se, ao receber nova mensagem, o AstraCalls consultar `GET /conversations/{conversation_id}` antes de criar a mensagem.

---

## 19. Configurações novas

Adicionar variáveis:

```env
CHATWOOT_REOPEN_RESOLVED_CONVERSATION=true
CHATWOOT_SOURCE_ID_STRATEGY=astra_session_phone
CHATWOOT_REOPEN_WINDOW_DAYS=0
CHATWOOT_USE_LOCAL_CONVERSATION_CACHE=true
CHATWOOT_DEDUP_ENABLED=true
```

Valores de `CHATWOOT_SOURCE_ID_STRATEGY`:

```text
phone_only             = whatsapp:{phone}
astra_session_phone    = astra:{session_id}:{phone}
custom                 = definido por função/configuração externa
```

Recomendação:

```env
CHATWOOT_SOURCE_ID_STRATEGY=astra_session_phone
```

---

## 20. Fluxo de criação inicial do contato

Quando o contato não existir:

```text
1. Criar contato no Chatwoot com telefone em E.164.
2. Criar contact_inbox com source_id estável.
3. Criar conversa com esse source_id.
4. Salvar mapeamento local.
```

Payload sugerido para contato:

```json
{
  "name": "Nome do WhatsApp ou telefone",
  "phone_number": "+5551999999999",
  "identifier": "5551999999999",
  "custom_attributes": {
    "source": "astracalls",
    "whatsapp_session": "comercial-01"
  }
}
```

Payload sugerido para conversa:

```json
{
  "source_id": "astra:comercial-01:5551999999999",
  "inbox_id": 10,
  "contact_id": 123,
  "status": "open",
  "custom_attributes": {
    "whatsapp_phone": "5551999999999",
    "astracalls_session_id": "comercial-01",
    "source_id_strategy": "astra_session_phone"
  }
}
```

---

## 21. Fluxo de reativação

### 21.1. Estado inicial

```text
chatwoot_contact_links:
phone = 5551999999999
source_id = astra:comercial-01:5551999999999
chatwoot_conversation_id = 456
last_conversation_status = resolved
```

### 21.2. Nova mensagem recebida

```text
Mensagem WhatsApp recebida
↓
AstraCalls normaliza telefone
↓
Encontra source_id
↓
Encontra conversation_id 456
↓
Consulta Chatwoot
↓
Status atual = resolved
↓
POST toggle_status { status: "open" }
↓
POST messages { message_type: "incoming" }
↓
Atualiza last_conversation_status = open
```

---

## 22. Exemplos cURL

### 22.1. Reabrir conversa

```bash
curl -X POST "https://chatwoot.seudominio.com/api/v1/accounts/1/conversations/456/toggle_status" \
  -H "Content-Type: application/json" \
  -H "api_access_token: ${CHATWOOT_API_TOKEN}" \
  -d '{
    "status": "open"
  }'
```

### 22.2. Criar mensagem incoming na conversa existente

```bash
curl -X POST "https://chatwoot.seudominio.com/api/v1/accounts/1/conversations/456/messages" \
  -H "Content-Type: application/json" \
  -H "api_access_token: ${CHATWOOT_API_TOKEN}" \
  -d '{
    "content": "Olá, gostaria de falar novamente com vocês.",
    "message_type": "incoming",
    "private": false,
    "content_type": "text",
    "content_attributes": {}
  }'
```

---

## 23. Critérios de aceite

A feature estará pronta quando:

- [ ] O mesmo telefone sempre gera o mesmo `source_id` na mesma sessão/inbox.
- [ ] Mensagem nova de contato com ticket resolvido reabre a conversa antiga.
- [ ] A nova mensagem entra na mesma `conversation_id`.
- [ ] Nenhuma conversa duplicada é criada para o mesmo contato/source_id.
- [ ] Existe idempotência contra mensagens duplicadas.
- [ ] Há lock para evitar race condition.
- [ ] O mapeamento local é atualizado.
- [ ] Se a conversa não existir, uma nova é criada normalmente.
- [ ] Se o Chatwoot estiver fora, o evento é retentado sem perder a mensagem.
- [ ] Logs mostram quando uma conversa foi reaberta.

---

## 24. Testes recomendados

### 24.1. Teste 1 — conversa nova

```text
Dado que o contato nunca falou antes
Quando ele envia mensagem
Então o sistema cria contato, conversa e mensagem
E salva source_id estável
```

### 24.2. Teste 2 — conversa resolvida

```text
Dado que o contato tem conversa resolvida
Quando ele envia nova mensagem
Então o sistema reabre a mesma conversa
E cria a mensagem nela
```

### 24.3. Teste 3 — mensagem duplicada

```text
Dado que a mesma mensagem chega duas vezes
Quando o handler processa o segundo evento
Então ele ignora por idempotência
```

### 24.4. Teste 4 — concorrência

```text
Dado que duas mensagens chegam ao mesmo tempo
Quando ambas são processadas
Então apenas uma conversa é criada/reaberta
E as duas mensagens ficam na mesma conversation_id
```

### 24.5. Teste 5 — múltiplas sessões

```text
Dado que o mesmo telefone fala com duas sessões AstraCalls diferentes
Quando cada sessão recebe mensagem
Então cada uma usa seu próprio source_id
E não mistura conversas entre inboxes/sessões
```

---

## 25. Logs recomendados

```text
chatwoot_source_id_built
chatwoot_contact_link_found
chatwoot_contact_link_created
chatwoot_conversation_found
chatwoot_conversation_created
chatwoot_conversation_reopened
chatwoot_message_created
chatwoot_message_duplicate_ignored
chatwoot_reopen_failed
```

Exemplo:

```json
{
  "level": "INFO",
  "msg": "chatwoot_conversation_reopened",
  "session_id": "comercial-01",
  "phone": "5551999999999",
  "source_id": "astra:comercial-01:5551999999999",
  "conversation_id": 456
}
```

---

## 26. Plano de implementação sugerido

### Etapa 1 — source_id estável

- Criar `NormalizePhone`.
- Criar `BuildSourceID`.
- Garantir que nenhum fluxo usa `message_id`, `call_id` ou timestamp como source_id.

### Etapa 2 — mapeamento local

- Criar tabela `chatwoot_contact_links`.
- Salvar `source_id`, `contact_id` e `conversation_id`.

### Etapa 3 — reabertura

- Antes de criar mensagem, buscar status da conversa.
- Se `resolved`, chamar `toggle_status` com `open`.

### Etapa 4 — idempotência

- Criar tabela `chatwoot_message_dedup`.
- Ignorar mensagens já processadas.

### Etapa 5 — observabilidade

- Logs estruturados.
- Métricas de reabertura.
- Alertas de duplicidade.

---

## 27. Referências

- Chatwoot — API Channel Inbox: https://www.chatwoot.com/hc/user-guide/articles/1677839703-how-to-create-an-api-channel-inbox
- Chatwoot — Create Conversation: https://developers.chatwoot.com/api-reference/conversations/create-new-conversation
- Chatwoot — Toggle Status: https://developers.chatwoot.com/api-reference/conversations/toggle-status
- Chatwoot — Conversation Details: https://developers.chatwoot.com/api-reference/conversations/conversation-details
- Chatwoot — Create Message: https://developers.chatwoot.com/api-reference/messages/create-new-message
- Chatwoot — Contact Conversations: https://developers.chatwoot.com/api-reference/contacts/contact-conversations
