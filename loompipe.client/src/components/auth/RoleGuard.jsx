import { useAuth } from '../../contexts/AuthContext';

const RoleGuard = ({ roles, children, fallback = null }) => {
    const { user } = useAuth();
    return user && roles.includes(user.role) ? children : fallback;
};

export default RoleGuard;
