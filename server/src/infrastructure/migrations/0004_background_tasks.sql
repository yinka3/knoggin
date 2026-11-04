-- Research jobs became background tasks, which can also run in normal mode.
ALTER TABLE public.background_work DROP CONSTRAINT background_work_kind_check;
UPDATE public.background_work SET kind = 'background_task' WHERE kind = 'research_job';
ALTER TABLE public.background_work ADD CONSTRAINT background_work_kind_check
    CHECK (kind = ANY (ARRAY['background_task', 'folder_notice']));
