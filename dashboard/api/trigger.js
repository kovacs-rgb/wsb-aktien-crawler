export default async function handler(req, res) {
  // CORS
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  const token = process.env.GH_PAT;
  if (!token) return res.status(500).json({ error: "GH_PAT nicht konfiguriert" });

  const owner = "kovacs-rgb";
  const repo = "wsb-aktien-crawler";

  try {
    const response = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/actions/workflows/pipeline.yml/dispatches`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/vnd.github.v3+json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ ref: "main" }),
      }
    );

    if (response.status === 204) {
      return res.status(200).json({ ok: true, message: "Pipeline gestartet" });
    }

    const text = await response.text();
    return res.status(response.status).json({ error: text });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
