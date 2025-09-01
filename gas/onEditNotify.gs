// Google Apps Script: двусторонняя синхронизация из Sheets → TG
// Работает по onEdit(e); читает изменённую строку и лист threads для подбора треда по первому лейблу.

const BOT_TOKEN = "123456:ABC...";            // токен бота
const CHAT_ID = "-1001234567890";             // id супер-группы
const TASKS_SHEET = "tasks";
const THREADS_SHEET = "threads";

function onEdit(e) {
  try {
    const range = e.range;
    const sheet = range.getSheet();
    if (sheet.getName() !== TASKS_SHEET) return;
    const row = range.getRow();
    if (row === 1) return; // шапка

    const headers = sheet.getRange(1,1,1,13).getValues()[0];
    const values = sheet.getRange(row,1,1,13).getValues()[0];
    const rowObj = Object.fromEntries(headers.map((h,i)=>[h, values[i]]));

    const changedCol = headers[range.getColumn()-1];

    // Определим тред по первому лейблу
    let threadId = null;
    const firstLabel = (rowObj.Labels || "").toString().split(",")[0].trim();
    if (firstLabel) {
      const wb = sheet.getParent();
      let ts = wb.getSheetByName(THREADS_SHEET);
      if (!ts) {
        ts = wb.insertSheet(THREADS_SHEET);
        ts.getRange(1,1,1,3).setValues([["Label","ThreadID","CreatedAt"]]);
      }
      const data = ts.getDataRange().getValues();
      for (let i=1; i<data.length; i++) {
        if ((data[i][0]||"").toString() === firstLabel) {
          threadId = (data[i][1]||"").toString();
          break;
        }
      }
    }

    const before = (e && e.oldValue !== undefined) ? e.oldValue : "(?)";
    const after  = (e && e.value !== undefined) ? e.value : rowObj[changedCol];

    const msg = [
      "\u270F\uFE0F <b>Изменение в Sheets</b>",
      `ID: <code>${rowObj.ID}</code>`,
      `Поле: <b>${changedCol}</b>`,
      `\u2192 <code>${before}</code> → <code>${after}</code>`,
      `Задача: ${rowObj.Title}`,
      firstLabel ? `#${firstLabel}` : ""
    ].filter(Boolean).join("\n");

    const url = `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`;
    const payload = {chat_id: CHAT_ID, text: msg, parse_mode: 'HTML'};
    if (threadId) payload.message_thread_id = Number(threadId);

    UrlFetchApp.fetch(url, {method:'post', contentType:'application/json', payload: JSON.stringify(payload)});
  } catch (err) {
    console.error(err);
  }
}
