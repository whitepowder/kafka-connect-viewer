// Applies the saved theme before the first paint; app.js handles switching.
(() => {
  let theme = null;
  try {
    theme = localStorage.getItem("kc-theme");
  } catch {
    theme = null;
  }
  if (theme !== "light" && theme !== "dark") {
    theme = typeof window.matchMedia === "function" && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  document.documentElement.dataset.theme = theme;
})();
