"""Worker side of history exports: renders a job's operations into CSV or PDF.

Files are written atomically (temp file + rename). A PDF is rendered in memory, so
it is limited to ``EXPORTS_PDF_MAX_ROWS`` operations; a larger period raises
:class:`ExportTooLargeError` (the job fails with a hint to use CSV). Kept apart
from :mod:`elbank.services.exports` so the API process does not load fpdf2.
"""

import asyncio
import csv
import io
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

from fpdf import FPDF
from fpdf.fonts import FontFace
from sqlalchemy import Select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import ExportSettings
from elbank.db.models import Account, Operation, PaymentProvider
from elbank.schemas.operation import Direction, ExportFormat, OperationRead
from elbank.services.exports import ExportJob, export_path
from elbank.services.operations import history_query, to_read, utc_bounds
from elbank.services.statements import safe_csv_cell

_STREAM_BATCH = 500
_CSV_HEADER = (
    "operation_id",
    "created_at",
    "account_number",
    "type",
    "category",
    "direction",
    "amount",
    "currency",
    "status",
    "counterparty_account",
    "description",
)
_PDF_HEADER = ("Дата (UTC)", "Счёт", "Тип", "Категория", "Сумма", "Валюта", "Статус", "Описание")
_PDF_COL_WIDTHS = (30, 40, 34, 26, 26, 14, 22, 85)  # mm, landscape A4 minus margins


class ExportTooLargeError(Exception):
    """The export does not fit the PDF row limit."""


def _signed_amount(item: OperationRead) -> str:
    sign = "+" if item.direction is Direction.CREDIT else "-"
    return f"{sign}{item.amount:.2f}"


def _csv_row(item: OperationRead) -> tuple[str, ...]:
    return (
        str(item.id),
        item.created_at.isoformat(),
        item.account_number,
        item.type.value,
        item.category.value if item.category else "",
        item.direction.value,
        f"{item.amount:.2f}",
        item.currency,
        item.status.value,
        safe_csv_cell(item.counterparty_account),
        safe_csv_cell(item.description),
    )


def _pdf_row(item: OperationRead) -> tuple[str, ...]:
    return (
        item.created_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M"),
        item.account_number,
        item.type.value,
        item.category.value if item.category else "",
        _signed_amount(item),
        item.currency,
        item.status.value,
        item.description or "",
    )


class ExportBuilder:
    """Worker side: render a job's operations into a file."""

    def __init__(self, settings: ExportSettings) -> None:
        self._settings = settings

    async def build(self, session: AsyncSession, job: ExportJob) -> int:
        """Write the file atomically; returns the number of operations.

        Raises :class:`ExportTooLargeError` for a PDF over the row limit.
        """
        path = export_path(self._settings, job)
        tmp = path.with_name(path.name + ".part")
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
        try:
            if job.format is ExportFormat.CSV:
                rows = await self._write_csv(session, job, tmp)
            else:
                rows = await self._write_pdf(session, job, tmp)
        except BaseException:
            await asyncio.to_thread(tmp.unlink, missing_ok=True)
            raise
        await asyncio.to_thread(tmp.replace, path)
        return rows

    @staticmethod
    def _query(job: ExportJob) -> Select[Operation, str, PaymentProvider]:
        start, end = utc_bounds(job.date_from, job.date_to)
        assert start is not None and end is not None
        return (
            history_query()
            .where(
                Account.user_id == job.user_id,
                Operation.created_at >= start,
                Operation.created_at < end,
            )
            .order_by(Operation.created_at, Operation.id)
            .execution_options(yield_per=_STREAM_BATCH)
        )

    async def _write_csv(self, session: AsyncSession, job: ExportJob, tmp: Path) -> int:
        file: TextIO = await asyncio.to_thread(tmp.open, "w", encoding="utf-8", newline="")
        try:
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(_CSV_HEADER)
            rows = 0
            result = await session.stream(self._query(job))
            async for batch in result.partitions(_STREAM_BATCH):
                for op, number, provider in batch:
                    writer.writerow(_csv_row(to_read(op, number, provider)))
                rows += len(batch)
                await asyncio.to_thread(file.write, buffer.getvalue())
                buffer.seek(0)
                buffer.truncate()
            await asyncio.to_thread(file.write, buffer.getvalue())
        finally:
            await asyncio.to_thread(file.close)
        return rows

    async def _write_pdf(self, session: AsyncSession, job: ExportJob, tmp: Path) -> int:
        limit = self._settings.pdf_max_rows
        items: list[tuple[str, ...]] = []
        result = await session.stream(self._query(job))
        async for batch in result.partitions(_STREAM_BATCH):
            items.extend(_pdf_row(to_read(op, number, provider)) for op, number, provider in batch)
            if len(items) > limit:
                await result.close()
                raise ExportTooLargeError(
                    f"More than {limit} operations: choose a shorter period or the CSV format"
                )
        await asyncio.to_thread(self._render_pdf, job, items, tmp)
        return len(items)

    def _render_pdf(self, job: ExportJob, rows: list[tuple[str, ...]], out: Path) -> None:
        pdf = FPDF(orientation="L", unit="mm", format="A4")
        pdf.set_auto_page_break(auto=True, margin=12)
        pdf.add_font("DejaVu", fname=self._settings.pdf_font_path)
        pdf.add_page()
        pdf.set_font("DejaVu", size=13)
        pdf.cell(
            text=f"История операций за {job.date_from:%d.%m.%Y} — {job.date_to:%d.%m.%Y}",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        pdf.set_font("DejaVu", size=8)
        pdf.cell(
            text=f"Сформировано {datetime.now(UTC):%d.%m.%Y %H:%M} UTC. Операций: {len(rows)}",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        pdf.ln(3)
        pdf.set_font("DejaVu", size=7)
        with pdf.table(
            col_widths=_PDF_COL_WIDTHS,
            headings_style=FontFace(emphasis="", fill_color=(225, 225, 225)),
            text_align="LEFT",
            line_height=4,
        ) as table:
            for values in (_PDF_HEADER, *rows):
                row = table.row()
                for value in values:
                    row.cell(value)
        pdf.output(str(out))

    async def remove_expired_files(self) -> int:
        """Delete files older than the job TTL (their jobs are gone from Redis)."""
        return await asyncio.to_thread(self._remove_expired_files_sync)

    def _remove_expired_files_sync(self) -> int:
        directory = self._settings.dir
        if not directory.is_dir():
            return 0
        deadline = time.time() - self._settings.job_ttl_seconds
        removed = 0
        for path in directory.iterdir():
            if path.suffix in {".csv", ".pdf", ".part"} and path.stat().st_mtime < deadline:
                path.unlink(missing_ok=True)
                removed += 1
        return removed
