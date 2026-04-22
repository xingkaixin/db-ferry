import { Link, useLocation, Outlet } from 'react-router-dom';
import { useStore } from '../state/store';

const navItems = [
  { path: '/', label: 'Dashboard', icon: '📊' },
  { path: '/config', label: 'Config', icon: '⚙️' },
  { path: '/history', label: 'History', icon: '📜' },
  { path: '/connections', label: 'Connections', icon: '🔌' },
];

export default function Layout() {
  const location = useLocation();
  const isConnected = useStore((s) => s.isConnected);

  return (
    <div className="flex h-screen bg-bg-primary text-text-primary">
      <aside className="w-56 border-r border-border bg-bg-secondary flex flex-col">
        <div className="p-4 border-b border-border">
          <h1 className="text-xl font-bold text-accent">db-ferry</h1>
          <div className="flex items-center gap-2 mt-2 text-sm">
            <span className={`w-2 h-2 rounded-full ${isConnected ? 'bg-success' : 'bg-danger'}`} />
            <span className="text-text-secondary">{isConnected ? 'Live' : 'Offline'}</span>
          </div>
        </div>
        <nav className="flex-1 p-2 space-y-1">
          {navItems.map((item) => (
            <Link
              key={item.path}
              to={item.path}
              className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors ${
                location.pathname === item.path
                  ? 'bg-bg-tertiary text-accent'
                  : 'text-text-secondary hover:bg-bg-tertiary hover:text-text-primary'
              }`}
            >
              <span>{item.icon}</span>
              <span>{item.label}</span>
            </Link>
          ))}
        </nav>
      </aside>
      <main className="flex-1 overflow-auto p-6">
        <Outlet />
      </main>
    </div>
  );
}
