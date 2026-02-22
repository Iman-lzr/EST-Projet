import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate, Link } from 'react-router-dom';
import Login from './components/Login';
import Dashboard from './components/Dashboard';
import AdminUsers from './components/AdminUsers'; // ← IMPORTE TON COMPOSANT

function App() {
  return (
    <Router>
      <div>
        {/* Barre de navigation pour tester */}
        <nav style={{ padding: '10px', backgroundColor: '#f0f0f0', marginBottom: '20px' }}>
          <Link to="/login" style={{ margin: '10px' }}>Login</Link>
          <Link to="/dashboard" style={{ margin: '10px' }}>Dashboard</Link>
          <Link to="/admin" style={{ margin: '10px' }}>Administration</Link> {/* ← LIEN VERS TA PAGE */}
        </nav>

        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/admin" element={<AdminUsers />} /> {/* ← TA ROUTE */}
          <Route path="/" element={<Navigate to="/login" />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;
