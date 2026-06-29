import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import App from "./App";
import RunListPage from "./pages/RunListPage";
import RunDetailPage from "./pages/RunDetailPage";
import WorkersPage from "./pages/WorkersPage";
import ClusterPage from "./pages/ClusterPage";
import DefinitionsPage from "./pages/DefinitionsPage";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<App />}>
          <Route index element={<RunListPage />} />
          <Route path="runs/:id" element={<RunDetailPage />} />
          <Route path="definitions" element={<DefinitionsPage />} />
          <Route path="workers" element={<WorkersPage />} />
          <Route path="cluster" element={<ClusterPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </React.StrictMode>,
);
