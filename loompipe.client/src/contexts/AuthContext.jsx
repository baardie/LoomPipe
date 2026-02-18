import React, { createContext, useCallback, useContext, useEffect, useState } from 'react';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
    const [user, setUser] = useState(null);
    const [token, setToken] = useState(() => localStorage.getItem('lp_token'));
    const [loading, setLoading] = useState(true);

    // Fetch current user info on mount / when token changes
    useEffect(() => {
        if (!token) {
            setUser(null);
            setLoading(false);
            return;
        }
        fetch('/api/auth/me', {
            headers: { Authorization: `Bearer ${token}` },
        })
            .then((r) => {
                if (!r.ok) throw new Error('Token invalid');
                return r.json();
            })
            .then((data) => setUser(data))
            .catch(() => {
                localStorage.removeItem('lp_token');
                setToken(null);
                setUser(null);
            })
            .finally(() => setLoading(false));
    }, [token]);

    const login = useCallback(async (username, password) => {
        const res = await fetch('/api/auth/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password }),
        });
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            throw new Error(err.message || 'Login failed');
        }
        const data = await res.json();
        localStorage.setItem('lp_token', data.token);
        setToken(data.token);
        setUser({ id: null, username: data.username, role: data.role });
    }, []);

    const logout = useCallback(() => {
        localStorage.removeItem('lp_token');
        setToken(null);
        setUser(null);
    }, []);

    const authFetch = useCallback(
        (url, options = {}) =>
            fetch(url, {
                ...options,
                headers: {
                    ...(options.headers || {}),
                    ...(token ? { Authorization: `Bearer ${token}` } : {}),
                },
            }),
        [token]
    );

    const isAdmin = user?.role === 'Admin';
    const isUser  = user?.role === 'Admin' || user?.role === 'User';
    const isGuest = !!user;

    return (
        <AuthContext.Provider value={{ user, loading, login, logout, authFetch, isAdmin, isUser, isGuest }}>
            {children}
        </AuthContext.Provider>
    );
}

export function useAuth() {
    return useContext(AuthContext);
}
