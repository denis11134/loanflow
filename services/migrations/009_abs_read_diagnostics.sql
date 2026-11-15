UPDATE abs_read
SET response_instruction_id = NULL
WHERE result = 'CORRELATION_ERROR' AND response_instruction_id IS NOT NULL;
