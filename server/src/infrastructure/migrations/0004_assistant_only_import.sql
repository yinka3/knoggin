-- A recorded assistant turn can start a session without an invented user message.
ALTER TABLE public.messages
    DROP CONSTRAINT messages_exchange_user_shape_check;
ALTER TABLE public.messages
    ADD CONSTRAINT messages_exchange_user_shape_check CHECK (
        (role = 'user' AND (
            (exchange_state = 'open' AND exchange_outcome IS NULL AND exchange_closed_at_ms IS NULL)
            OR (exchange_state = 'closed' AND exchange_outcome IN
                ('assistant_final', 'clarification', 'failed', 'cancelled', 'user_only')
                AND exchange_closed_at_ms IS NOT NULL AND exchange_closed_at_ms >= 0)
        ))
        OR (role = 'assistant' AND (
            (exchange_state = 'open' AND exchange_outcome IS NULL AND exchange_closed_at_ms IS NULL)
            OR (exchange_state = 'closed' AND exchange_outcome = 'assistant_only'
                AND user_msg_id IS NULL
                AND exchange_closed_at_ms IS NOT NULL AND exchange_closed_at_ms >= 0)
        ))
    );
