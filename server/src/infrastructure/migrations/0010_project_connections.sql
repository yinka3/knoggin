-- Saved logins (Hugging Face, Kaggle, GitHub) that this project's approved
-- sandbox commands may use. The secrets themselves are never stored here.
ALTER TABLE public.projects ADD COLUMN connections text[] DEFAULT '{}'::text[] NOT NULL;
