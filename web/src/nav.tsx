import * as React from "react";
import { Link, matchPath, useLocation } from "react-router-dom";
import { AuthState } from "./index";

function AccountLink({ auth }: { auth: AuthState }) {
  const location = useLocation();
  const returnTo = encodeURIComponent(
    location.pathname + location.search + location.hash,
  );

  if (auth.type === "loading" || matchPath(location.pathname, "/authorized")) {
    return <span className="nav-status">logging in…</span>;
  }
  if (auth.type === "missing") {
    return <Link to={`/login?returnTo=${returnTo}`}>log in</Link>;
  }
  if (auth.userDetailsValidating) {
    return <span className="nav-status">checking login…</span>;
  }
  return (
    <span className="account-links">
      <img className="profile-image" src={auth.userProfileImageUrl} alt="" />
      <Link to="/settings">{auth.userName}</Link>
      <span aria-hidden="true">·</span>
      <Link to={`/logout?returnTo=${returnTo}`}>log out</Link>
    </span>
  );
}

export function NavWithRouter({ auth }: { auth: AuthState }) {
  return (
    <header className="site-header">
      <nav className="site-nav" aria-label="Main navigation">
        <span className="nav-links">
          <Link to="/">home</Link>
          <Link to="/viewer">viewer</Link>
          <Link to="/settings">settings</Link>
        </span>
        <AccountLink auth={auth} />
      </nav>
    </header>
  );
}
