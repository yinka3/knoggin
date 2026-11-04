-- Tool work that keeps running in the background after its run ends.
ALTER TABLE public.background_work DROP CONSTRAINT background_work_kind_check;
ALTER TABLE public.background_work ADD CONSTRAINT background_work_kind_check
    CHECK (kind = ANY (ARRAY['background_task', 'folder_notice', 'tool_handoff']));
