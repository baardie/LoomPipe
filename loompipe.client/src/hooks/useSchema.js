import { useState, useEffect } from 'react';

const useSchema = (sourceType, sourceConnectionString, destinationSchemaText) => {
    const [sourceSchema, setSourceSchema] = useState([]);
    const [destinationSchema, setDestinationSchema] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    // Resolve destination schema from the comma-separated text field
    useEffect(() => {
        const fields = destinationSchemaText
            ? destinationSchemaText.split(',').map(s => s.trim()).filter(Boolean)
            : [];
        setDestinationSchema(fields);
    }, [destinationSchemaText]);

    // Resolve source schema from the API
    useEffect(() => {
        if (!sourceType || !sourceConnectionString) {
            setSourceSchema([]);
            return;
        }

        let cancelled = false;
        const fetchSchema = async () => {
            setLoading(true);
            setError(null);
            try {
                const response = await fetch('/api/schema/source', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type: sourceType, connectionString: sourceConnectionString }),
                });
                if (!response.ok) throw new Error(`Schema discovery failed: ${response.status}`);
                const data = await response.json();
                if (!cancelled) setSourceSchema(data);
            } catch (e) {
                if (!cancelled) setError(e);
            } finally {
                if (!cancelled) setLoading(false);
            }
        };

        fetchSchema();
        return () => { cancelled = true; };
    }, [sourceType, sourceConnectionString]);

    return { sourceSchema, destinationSchema, loading, error };
};

export default useSchema;
