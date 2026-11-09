-- PostgreSQL marks a stored generated column as updated when a BEFORE UPDATE
-- trigger is present. Guard the document inputs and compare its hash by derivation.
DROP TRIGGER individual_terms_document_frozen ON individual_terms;
CREATE TRIGGER individual_terms_document_frozen
    BEFORE UPDATE OF terms_id, offer_id, application_id, document_bytes, content_type,
        first_provided_at, offer_expires_at OR DELETE ON individual_terms
    FOR EACH ROW EXECUTE FUNCTION prevent_frozen_row_change();
