import hashlib
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from fastapi import HTTPException
from starlette.requests import Request
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.models import (
    Base,
    B2BAROpeningBalance,
    B2BARPayment,
    B2BCommercialAgreement,
    B2BCommercialAgreementPerson,
    B2BCommercialAgreementSection,
    B2BCustomer,
    B2BCustomerProduct,
    B2BSalesOrder,
    Channel,
    InventoryBalance,
    InventoryTransaction,
    Permission,
    Role,
    RolePermission,
)
from app.services.auth_service import ensure_auth_seed_state, require_permission
from app.services.b2b_commercial_agreement_service import (
    B2BCommercialAgreementValidationError,
    DEFAULT_AGREEMENT_SECTIONS,
    DEFAULT_AGREEMENT_SECTION_TEXTS,
    PAYMENT_TERM_OPTIONS,
    USUAL_DELIVERY_TYPE_OPTIONS,
    create_commercial_agreement_draft,
    find_missing_template_variables,
    format_person_contact_info,
    get_commercial_agreement,
    normalize_payment_terms,
    parse_person_contact_info,
    render_agreement_template_text,
    reset_commercial_agreement_sections_to_template,
    update_commercial_agreement_draft,
    validate_commercial_agreement,
)
from app.services.b2b_commercial_agreement_pdf_service import (
    B2BCommercialAgreementIncompleteError,
    build_b2b_commercial_agreement_pdf,
    generate_and_store_b2b_commercial_agreement_pdf,
    get_stored_b2b_commercial_agreement_pdf,
    safe_b2b_commercial_agreement_pdf_filename,
)


class B2BCommercialAgreementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.Session.begin() as db:
            channel = Channel(
                name="Tiendas Naturales",
                active=True,
                applies_to_b2b=True,
            )
            db.add(channel)
            db.flush()
            customer = B2BCustomer(
                customer_name="Cliente Acuerdo",
                legal_name="Cliente Acuerdo S.A.",
                legal_id="3-101-123456",
                phone="8888-0000",
                address="San José centro",
                province="San José",
                canton="San José",
                district="Carmen",
                credit_days=15,
                channel_id=channel.id,
                active=True,
            )
            db.add(customer)
            db.flush()
            self.customer_id = customer.id

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _create(self, db) -> B2BCommercialAgreement:
        return create_commercial_agreement_draft(
            db,
            customer_id=self.customer_id,
            user_id=None,
        )

    def _complete(self, db) -> B2BCommercialAgreement:
        agreement = self._create(db)
        return update_commercial_agreement_draft(
            db,
            agreement_id=agreement.id,
            fields=self._complete_fields(),
            people=self._people(),
            section_updates={
                section.section_key: {
                    "title": section.title,
                    "body_text": section.body_text,
                }
                for section in agreement.sections
            },
            user_id=None,
        )

    @staticmethod
    def _complete_fields() -> dict[str, object]:
        return {
            "legal_representative_name": "Laura Cliente",
            "legal_representative_id": "1-1111-1111",
            "legal_representative_position": "Gerente",
            "billing_address": "San José centro",
            "billing_email": "facturas@example.com",
            "administrative_email": "admin@example.com",
            "whatsapp": "8888-0000",
            "b2b_discount_percent": "12.5",
            "payment_terms": "Crédito comercial aprobado",
            "contract_credit_days": "15",
            "credit_limit": "250000",
            "order_reception_days": "Lunes a viernes",
            "usual_prep_time": "Dos días hábiles",
            "usual_delivery_type": "Ruta de entrega GREEN CORNER",
            "signature_place": "San José",
            "signature_date": "2026-10-14",
            "client_signer_name": "Laura Cliente",
            "client_signer_id": "1-1111-1111",
            "client_signer_position": "Gerente",
            "catalog_reference": "Ver catálogo comercial vigente adjunto.",
        }

    @staticmethod
    def _people() -> list[dict[str, str]]:
        return [
            {
                "authorization_type": "orders",
                "name": "Persona Pedidos",
                "identification": "1",
                "position": "Compras",
                "phone": "8000-0001",
                "email": "pedidos@example.com",
            },
            {
                "authorization_type": "receiving",
                "name": "Persona Recepción",
                "identification": "2",
                "position": "Bodega",
                "phone": "8000-0002",
                "email": "recibe@example.com",
            },
        ]

    @staticmethod
    def _section_updates(agreement: B2BCommercialAgreement) -> dict[str, dict[str, str]]:
        return {
            section.section_key: {
                "title": section.title,
                "body_text": f"Texto revisado para {section.title}.",
            }
            for section in agreement.sections
        }

    def test_create_version_zero_snapshots_customer_and_creates_19_sections(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            self.assertEqual(agreement.version_number, 0)
            self.assertEqual(agreement.status, "draft")
            self.assertEqual(agreement.customer_name_snapshot, "Cliente Acuerdo")
            self.assertEqual(agreement.legal_name_snapshot, "Cliente Acuerdo S.A.")
            self.assertEqual(agreement.credit_days_snapshot, 15)
            self.assertEqual(agreement.b2b_channel_name_snapshot, "Tiendas Naturales")
            self.assertTrue(agreement.customer_active_snapshot)
            self.assertEqual(len(agreement.sections), 19)
            self.assertEqual(
                [section.title for section in agreement.sections],
                [title for _key, title in DEFAULT_AGREEMENT_SECTIONS],
            )
            self.assertTrue(all(section.body_text.strip() for section in agreement.sections))
            self.assertFalse(
                any("Texto base editable" in section.body_text for section in agreement.sections)
            )
            self.assertEqual(
                {section.section_key: section.body_text for section in agreement.sections},
                DEFAULT_AGREEMENT_SECTION_TEXTS,
            )
            self.assertTrue(
                any("{{customer_name}}" in section.body_text for section in agreement.sections)
            )
            self.assertTrue(
                any("{{credit_limit}}" in section.body_text for section in agreement.sections)
            )
            self.assertIsNone(agreement.generated_pdf_bytes)
            self.assertIsNone(agreement.generated_pdf_filename)
            self.assertIsNone(agreement.generated_pdf_sha256)

    def test_customer_changes_do_not_alter_agreement_snapshots(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            customer = db.get(B2BCustomer, self.customer_id)
            customer.customer_name = "Nombre posterior"
            customer.legal_name = "Razón posterior"
            customer.active = False
            db.commit()
            db.expire_all()

            unchanged = get_commercial_agreement(db, agreement.id)
            self.assertEqual(unchanged.customer_name_snapshot, "Cliente Acuerdo")
            self.assertEqual(unchanged.legal_name_snapshot, "Cliente Acuerdo S.A.")
            self.assertTrue(unchanged.customer_active_snapshot)

    def test_only_one_draft_is_allowed_per_customer(self) -> None:
        with self.Session() as db:
            self._create(db)
            with self.assertRaisesRegex(
                B2BCommercialAgreementValidationError,
                "ya tiene un acuerdo comercial en borrador",
            ):
                self._create(db)

    def test_people_and_sections_are_saved_for_draft(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            updated = update_commercial_agreement_draft(
                db,
                agreement_id=agreement.id,
                fields=self._complete_fields(),
                people=self._people(),
                section_updates=self._section_updates(agreement),
                user_id=None,
            )
            self.assertEqual(len(updated.authorized_people), 2)
            self.assertEqual(
                {person.authorization_type for person in updated.authorized_people},
                {"orders", "receiving"},
            )
            self.assertTrue(all(section.body_text.startswith("Texto revisado") for section in updated.sections))
            self.assertEqual(validate_commercial_agreement(updated), [])

    def test_only_draft_can_be_edited(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            agreement.status = "generated"
            db.commit()
            with self.assertRaisesRegex(B2BCommercialAgreementValidationError, "Solo los acuerdos"):
                update_commercial_agreement_draft(
                    db,
                    agreement_id=agreement.id,
                    fields=self._complete_fields(),
                    people=self._people(),
                    section_updates=self._section_updates(agreement),
                    user_id=None,
                )

    def test_validation_returns_full_missing_list_and_credit_requirements(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            agreement.legal_name_snapshot = None
            agreement.address_snapshot = None
            agreement.customer_active_snapshot = False
            agreement.payment_terms = "Crédito comercial aprobado"
            agreement.credit_approved = True
            agreement.contract_credit_days = None
            agreement.credit_limit = Decimal("0")
            agreement.sections[0].title = ""
            agreement.sections[1].body_text = ""
            db.flush()

            missing = validate_commercial_agreement(agreement)
            self.assertIn("El cliente debe estar activo en el snapshot del acuerdo.", missing)
            self.assertIn("Razón social", missing)
            self.assertIn("Dirección", missing)
            self.assertIn("Representante legal", missing)
            self.assertIn("Al menos una persona autorizada para pedidos", missing)
            self.assertIn("Al menos una persona autorizada para recibir mercadería", missing)
            self.assertIn("Los días de crédito contractual deben ser mayores a 0.", missing)
            self.assertIn("El límite de crédito debe ser mayor a 0.", missing)
            self.assertIn("Título del apartado 1", missing)
            self.assertIn("Texto del apartado 2: Descuento comercial B2B y volumen de compra", missing)

    def test_payment_and_delivery_choices_are_validated_and_drive_flags(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            fields = self._complete_fields()
            fields["payment_terms"] = "Pago anticipado antes del despacho"
            fields["usual_delivery_type"] = "Encomienda"
            updated = update_commercial_agreement_draft(
                db,
                agreement_id=agreement.id,
                fields=fields,
                people=self._people(),
                section_updates=self._section_updates(agreement),
                user_id=None,
            )
            self.assertEqual(updated.payment_terms, "Pago anticipado antes del despacho")
            self.assertEqual(updated.usual_delivery_type, "Encomienda")
            self.assertTrue(updated.advance_payment_enabled)
            self.assertFalse(updated.credit_approved)

            fields["payment_terms"] = "Forma inventada"
            with self.assertRaisesRegex(
                B2BCommercialAgreementValidationError,
                "Forma de pago debe ser una de las opciones permitidas",
            ):
                update_commercial_agreement_draft(
                    db,
                    agreement_id=agreement.id,
                    fields=fields,
                    people=self._people(),
                    section_updates=self._section_updates(agreement),
                    user_id=None,
                )
            db.rollback()

            fields = self._complete_fields()
            fields["usual_delivery_type"] = "Entrega inventada"
            with self.assertRaisesRegex(
                B2BCommercialAgreementValidationError,
                "Entrega habitual debe ser una de las opciones permitidas",
            ):
                update_commercial_agreement_draft(
                    db,
                    agreement_id=agreement.id,
                    fields=fields,
                    people=self._people(),
                    section_updates=self._section_updates(agreement),
                    user_id=None,
                )

    def test_credit_requires_terms_and_limit_but_cash_does_not(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            fields = self._complete_fields()
            fields["contract_credit_days"] = ""
            fields["credit_limit"] = ""
            credit = update_commercial_agreement_draft(
                db,
                agreement_id=agreement.id,
                fields=fields,
                people=self._people(),
                section_updates=self._section_updates(agreement),
                user_id=None,
            )
            credit_missing = validate_commercial_agreement(credit)
            self.assertIn("Los días de crédito contractual deben ser mayores a 0.", credit_missing)
            self.assertIn("El límite de crédito debe ser mayor a 0.", credit_missing)

            fields["payment_terms"] = "Contado contra entrega"
            cash = update_commercial_agreement_draft(
                db,
                agreement_id=agreement.id,
                fields=fields,
                people=self._people(),
                section_updates=self._section_updates(credit),
                user_id=None,
            )
            self.assertFalse(cash.credit_approved)
            self.assertFalse(cash.advance_payment_enabled)
            self.assertNotIn(
                "Los días de crédito contractual deben ser mayores a 0.",
                validate_commercial_agreement(cash),
            )
            self.assertNotIn(
                "El límite de crédito debe ser mayor a 0.",
                validate_commercial_agreement(cash),
            )

    def test_legacy_payment_aliases_and_person_contact_round_trip(self) -> None:
        self.assertEqual(normalize_payment_terms("Contado"), "Contado contra entrega")
        self.assertEqual(normalize_payment_terms("Crédito"), "Crédito comercial aprobado")
        combined = format_person_contact_info("8888-0000", "persona@example.com")
        self.assertEqual(combined, "8888-0000 / persona@example.com")
        self.assertEqual(
            parse_person_contact_info(combined),
            ("8888-0000", "persona@example.com"),
        )
        self.assertEqual(parse_person_contact_info("persona@example.com"), (None, "persona@example.com"))
        self.assertEqual(parse_person_contact_info("8888-0000"), ("8888-0000", None))

        with self.Session() as db:
            agreement = self._create(db)
            people_without_identification = [
                {**person, "identification": ""}
                for person in self._people()
            ]
            updated = update_commercial_agreement_draft(
                db,
                agreement_id=agreement.id,
                fields=self._complete_fields(),
                people=people_without_identification,
                section_updates=self._section_updates(agreement),
                user_id=None,
            )
            self.assertTrue(all(person.identification is None for person in updated.authorized_people))
            self.assertTrue(all(person.phone and person.email for person in updated.authorized_people))

            missing_contact = [dict(people_without_identification[0], phone="", email="")]
            with self.assertRaisesRegex(
                B2BCommercialAgreementValidationError,
                "Teléfono / correo es obligatorio",
            ):
                update_commercial_agreement_draft(
                    db,
                    agreement_id=agreement.id,
                    fields=self._complete_fields(),
                    people=missing_contact,
                    section_updates=self._section_updates(updated),
                    user_id=None,
                )

    def test_template_renderer_handles_values_missing_and_unknown_variables(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            agreement.legal_representative_name = "Ana Representante"
            agreement.credit_limit = Decimal("50000")
            agreement.signature_date = date(2026, 10, 14)

            rendered = render_agreement_template_text(
                "Cliente: {{customer_name}} | Representante: {{legal_representative_name}} | "
                "Crédito: ₡{{credit_limit}} | Firma: {{signature_date}} | "
                "Correo: {{billing_email}} | Otro: {{variable_desconocida}}",
                agreement,
            )
            self.assertIn("Cliente: Cliente Acuerdo", rendered)
            self.assertIn("Representante: Ana Representante", rendered)
            self.assertIn("Crédito: ₡50,000", rendered)
            self.assertIn("Firma: 2026-10-14", rendered)
            self.assertIn("Correo: ______", rendered)
            self.assertIn("Otro: {{variable_desconocida}}", rendered)
            self.assertEqual(
                find_missing_template_variables(
                    "{{billing_email}} {{billing_email}} {{variable_desconocida}}",
                    agreement,
                ),
                ["billing_email"],
            )
            validation_messages = validate_commercial_agreement(agreement)
            self.assertTrue(
                any("{{billing_email}}" in message for message in validation_messages)
            )

    def test_reset_template_replaces_only_sections_without_duplicates(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            updated = update_commercial_agreement_draft(
                db,
                agreement_id=agreement.id,
                fields=self._complete_fields(),
                people=self._people(),
                section_updates=self._section_updates(agreement),
                user_id=None,
            )
            section_identity = {
                section.section_key: (
                    section.id,
                    section.section_number,
                    section.display_order,
                )
                for section in updated.sections
            }
            protected_values = {
                "customer_name_snapshot": updated.customer_name_snapshot,
                "legal_name_snapshot": updated.legal_name_snapshot,
                "b2b_discount_percent": updated.b2b_discount_percent,
                "credit_limit": updated.credit_limit,
                "signature_place": updated.signature_place,
                "signature_date": updated.signature_date,
            }
            people_before = [
                (
                    person.authorization_type,
                    person.name,
                    person.identification,
                    person.position,
                    person.phone,
                    person.email,
                )
                for person in updated.authorized_people
            ]

            reset = reset_commercial_agreement_sections_to_template(
                db,
                agreement_id=updated.id,
            )
            self.assertEqual(len(reset.sections), 19)
            self.assertEqual(len({section.section_key for section in reset.sections}), 19)
            self.assertEqual(
                {section.section_key: section.body_text for section in reset.sections},
                DEFAULT_AGREEMENT_SECTION_TEXTS,
            )
            self.assertEqual(
                {
                    section.section_key: (
                        section.id,
                        section.section_number,
                        section.display_order,
                    )
                    for section in reset.sections
                },
                section_identity,
            )
            for field_name, expected in protected_values.items():
                self.assertEqual(getattr(reset, field_name), expected)
            self.assertEqual(
                [
                    (
                        person.authorization_type,
                        person.name,
                        person.identification,
                        person.position,
                        person.phone,
                        person.email,
                    )
                    for person in reset.authorized_people
                ],
                people_before,
            )

            reset_again = reset_commercial_agreement_sections_to_template(
                db,
                agreement_id=reset.id,
            )
            self.assertEqual(len(reset_again.sections), 19)
            self.assertEqual(len({section.section_key for section in reset_again.sections}), 19)

    def test_reset_template_is_blocked_for_every_non_draft_status(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            for status in ("generated", "signed", "archived"):
                with self.subTest(status=status):
                    agreement.status = status
                    db.commit()
                    with self.assertRaisesRegex(
                        B2BCommercialAgreementValidationError,
                        "Solo los acuerdos comerciales en estado draft",
                    ):
                        reset_commercial_agreement_sections_to_template(
                            db,
                            agreement_id=agreement.id,
                        )
                    agreement.status = "draft"
                    db.commit()

    def test_agreement_flow_does_not_query_products_or_modify_restricted_tables(self) -> None:
        statements: list[str] = []

        def record_statement(_connection, _cursor, statement, _parameters, _context, _many) -> None:
            statements.append(statement.lower())

        event.listen(self.engine, "before_cursor_execute", record_statement)
        try:
            with self.Session() as db:
                before = {
                    "products": db.query(B2BCustomerProduct).count(),
                    "orders": db.query(B2BSalesOrder).count(),
                    "payments": db.query(B2BARPayment).count(),
                    "opening_balances": db.query(B2BAROpeningBalance).count(),
                    "inventory_transactions": db.query(InventoryTransaction).count(),
                    "inventory_balances": db.query(InventoryBalance).count(),
                }
                statements.clear()
                agreement = self._create(db)
                get_commercial_agreement(db, agreement.id)
                reset_commercial_agreement_sections_to_template(
                    db,
                    agreement_id=agreement.id,
                )
                service_statements = list(statements)
                after = {
                    "products": db.query(B2BCustomerProduct).count(),
                    "orders": db.query(B2BSalesOrder).count(),
                    "payments": db.query(B2BARPayment).count(),
                    "opening_balances": db.query(B2BAROpeningBalance).count(),
                    "inventory_transactions": db.query(InventoryTransaction).count(),
                    "inventory_balances": db.query(InventoryBalance).count(),
                }
                self.assertEqual(before, after)
        finally:
            event.remove(self.engine, "before_cursor_execute", record_statement)

        joined_sql = "\n".join(service_statements)
        self.assertNotIn("b2b_customer_products", joined_sql)
        self.assertNotIn(" from products", joined_sql)
        self.assertNotIn("inventory_transactions", joined_sql)
        self.assertNotIn("inventory_balances", joined_sql)
        self.assertNotIn("b2b_sales_orders", joined_sql)
        self.assertNotIn("b2b_ar_payments", joined_sql)
        self.assertNotIn("b2b_ar_opening_balances", joined_sql)

    def test_permission_assignments_and_route_guards(self) -> None:
        with self.Session.begin() as db:
            ensure_auth_seed_state(db)

        def role_permissions(db, role_code: str) -> set[str]:
            return {
                code
                for (code,) in (
                    db.query(Permission.code)
                    .join(RolePermission, RolePermission.permission_id == Permission.id)
                    .join(Role, Role.id == RolePermission.role_id)
                    .filter(Role.code == role_code)
                    .all()
                )
            }

        agreement_permissions = {
            "b2b_commercial_agreement.view",
            "b2b_commercial_agreement.edit",
            "b2b_commercial_agreement.generate",
            "b2b_commercial_agreement.sign_archive",
        }
        with self.Session() as db:
            self.assertTrue(agreement_permissions.issubset(role_permissions(db, "admin")))
            self.assertEqual(
                role_permissions(db, "sales_manager") & agreement_permissions,
                agreement_permissions - {"b2b_commercial_agreement.sign_archive"},
            )
            self.assertTrue(role_permissions(db, "general_operator").isdisjoint(agreement_permissions))
            self.assertEqual(
                role_permissions(db, "general_approver") & agreement_permissions,
                agreement_permissions,
            )

        main_source = Path("app/main.py").read_text(encoding="utf-8")
        self.assertIn('require_permission(request, "b2b_commercial_agreement.view")', main_source)
        self.assertIn('require_permission(request, "b2b_commercial_agreement.edit")', main_source)
        self.assertIn('require_permission(request, "b2b_commercial_agreement.generate")', main_source)
        self.assertGreaterEqual(
            main_source.count('detail="Solo los acuerdos en borrador se pueden editar."'),
            3,
        )
        self.assertIn(
            '@app.post("/b2b/commercial-agreements/{agreement_id}/sections/reset-template")',
            main_source,
        )
        reset_route_source = main_source.split(
            '@app.post("/b2b/commercial-agreements/{agreement_id}/sections/reset-template")',
            1,
        )[1].split('@app.post("/b2b/commercial-agreements/{agreement_id}/validate"', 1)[0]
        self.assertIn(
            'require_permission(request, "b2b_commercial_agreement.edit")',
            reset_route_source,
        )
        self.assertIn('action="commercial_agreement_sections_reset"', reset_route_source)

    def test_reset_route_rejects_user_without_edit_permission_with_403(self) -> None:
        request = Request({"type": "http", "method": "POST", "path": "/", "headers": []})
        request.state.current_user = SimpleNamespace(id=123)
        request.state.current_permissions = set()
        with self.assertRaises(HTTPException) as raised:
            require_permission(request, "b2b_commercial_agreement.edit")
        self.assertEqual(raised.exception.status_code, 403)

    def test_actions_and_pdf_routes_are_permission_guarded(self) -> None:
        customer_template = Path("app/templates/b2b_customers_list.html").read_text(encoding="utf-8")
        editor_template = Path("app/templates/b2b_commercial_agreement_edit.html").read_text(
            encoding="utf-8"
        )
        main_source = Path("app/main.py").read_text(encoding="utf-8")
        service_source = Path("app/services/b2b_commercial_agreement_service.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("Acuerdo comercial", customer_template)
        self.assertIn('can(request, "b2b_commercial_agreement.view")', customer_template)
        self.assertIn(
            '@app.post("/b2b/commercial-agreements/{agreement_id}/generate-pdf")',
            main_source,
        )
        self.assertIn(
            '@app.get("/b2b/commercial-agreements/{agreement_id}/download-pdf")',
            main_source,
        )
        generate_route = main_source.split(
            '@app.post("/b2b/commercial-agreements/{agreement_id}/generate-pdf")',
            1,
        )[1].split(
            '@app.get("/b2b/commercial-agreements/{agreement_id}/download-pdf")',
            1,
        )[0]
        download_route = main_source.split(
            '@app.get("/b2b/commercial-agreements/{agreement_id}/download-pdf")',
            1,
        )[1].split(
            '@app.get("/b2b/commercial-agreements/{agreement_id}"',
            1,
        )[0]
        self.assertIn(
            'require_permission(request, "b2b_commercial_agreement.generate")',
            generate_route,
        )
        self.assertIn(
            'require_permission(request, "b2b_commercial_agreement.view")',
            download_route,
        )
        self.assertIn("get_stored_b2b_commercial_agreement_pdf(agreement)", download_route)
        self.assertIn("content=pdf_bytes", download_route)
        self.assertIn('media_type="application/pdf"', download_route)
        self.assertIn('action="commercial_agreement_generated"', generate_route)
        self.assertNotIn("reportlab", service_source.lower())
        self.assertNotIn("b2b_catalog_export_service", service_source)
        self.assertNotIn("Product", service_source)
        self.assertIn("Actualizar textos desde plantilla base", editor_template)
        self.assertIn("sections/reset-template", editor_template)
        self.assertIn('<select name="payment_terms" required>', editor_template)
        self.assertIn('<select name="usual_delivery_type" required>', editor_template)
        self.assertIn("{% for option in payment_term_options %}", editor_template)
        self.assertIn("{% for option in usual_delivery_type_options %}", editor_template)
        self.assertIn('"payment_term_options": PAYMENT_TERM_OPTIONS', main_source)
        self.assertIn('"usual_delivery_type_options": USUAL_DELIVERY_TYPE_OPTIONS', main_source)
        for option in PAYMENT_TERM_OPTIONS + USUAL_DELIVERY_TYPE_OPTIONS:
            self.assertIn(option, service_source)
        self.assertIn("Teléfono / correo", editor_template)
        self.assertNotIn("<th>Identificación</th>", editor_template)
        self.assertNotIn('name="{{ kind }}_person_phone"', editor_template)
        self.assertNotIn('name="{{ kind }}_person_email"', editor_template)
        self.assertIn('type="hidden" name="{{ kind }}_person_identification"', editor_template)
        self.assertNotIn('name="section_title_{{ section.section_key }}" value="{{ section.title }}" required', editor_template)
        self.assertNotIn('name="section_body_{{ section.section_key }}" rows="6" required', editor_template)
        self.assertIn("Generar PDF", editor_template)
        self.assertIn("generate-pdf", editor_template)
        detail_template = Path("app/templates/b2b_commercial_agreement_detail.html").read_text(
            encoding="utf-8"
        )
        history_template = Path("app/templates/b2b_commercial_agreements.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("Descargar PDF", detail_template)
        self.assertIn("download-pdf", detail_template)
        self.assertIn("Descargar PDF", history_template)
        self.assertIn("download-pdf", history_template)

    def test_complete_draft_generates_and_persists_immutable_pdf_metadata(self) -> None:
        with self.Session() as db:
            agreement = self._complete(db)
            generated = generate_and_store_b2b_commercial_agreement_pdf(
                db,
                agreement_id=agreement.id,
                user_id=None,
                logo_bytes=None,
            )

            self.assertEqual(generated.status, "generated")
            self.assertIsNotNone(generated.generated_at)
            self.assertIsNone(generated.generated_by_user_id)
            self.assertTrue(generated.generated_pdf_bytes.startswith(b"%PDF"))
            self.assertGreater(len(generated.generated_pdf_bytes), 5000)
            self.assertEqual(
                generated.generated_pdf_filename,
                "Acuerdo_Comercial_B2B_Cliente_Acuerdo_v0.pdf",
            )
            self.assertEqual(
                generated.generated_pdf_sha256,
                hashlib.sha256(generated.generated_pdf_bytes).hexdigest(),
            )
            stored_bytes, stored_filename = get_stored_b2b_commercial_agreement_pdf(generated)
            self.assertEqual(stored_bytes, generated.generated_pdf_bytes)
            self.assertEqual(stored_filename, generated.generated_pdf_filename)

    def test_incomplete_draft_lists_all_missing_fields_and_writes_nothing(self) -> None:
        with self.Session() as db:
            agreement = self._create(db)
            with self.assertRaises(B2BCommercialAgreementIncompleteError) as raised:
                generate_and_store_b2b_commercial_agreement_pdf(
                    db,
                    agreement_id=agreement.id,
                    user_id=55,
                )
            self.assertGreater(len(raised.exception.missing_fields), 5)
            db.expire_all()
            unchanged = get_commercial_agreement(db, agreement.id)
            self.assertEqual(unchanged.status, "draft")
            self.assertIsNone(unchanged.generated_pdf_bytes)
            self.assertIsNone(unchanged.generated_pdf_filename)
            self.assertIsNone(unchanged.generated_pdf_sha256)
            self.assertIsNone(unchanged.generated_at)
            self.assertIsNone(unchanged.generated_by_user_id)

    def test_generated_signed_and_archived_cannot_regenerate(self) -> None:
        with self.Session() as db:
            agreement = self._complete(db)
            for status in ("generated", "signed", "archived"):
                with self.subTest(status=status):
                    agreement.status = status
                    db.commit()
                    pdf_builder = mock.Mock(return_value=b"%PDF-should-not-run")
                    with self.assertRaisesRegex(
                        B2BCommercialAgreementValidationError,
                        "Solo los acuerdos comerciales en estado draft",
                    ):
                        generate_and_store_b2b_commercial_agreement_pdf(
                            db,
                            agreement_id=agreement.id,
                            user_id=55,
                            pdf_builder=pdf_builder,
                        )
                    pdf_builder.assert_not_called()
                    agreement = get_commercial_agreement(db, agreement.id)

    def test_generation_failure_rolls_back_status_and_pdf_fields(self) -> None:
        with self.Session() as db:
            agreement = self._complete(db)
            original_updated_at = agreement.updated_at

            with mock.patch.object(db, "commit", side_effect=RuntimeError("commit failed")):
                with self.assertRaisesRegex(RuntimeError, "commit failed"):
                    generate_and_store_b2b_commercial_agreement_pdf(
                        db,
                        agreement_id=agreement.id,
                        user_id=55,
                        pdf_builder=mock.Mock(return_value=b"%PDF-controlled-test"),
                    )

            db.expire_all()
            unchanged = get_commercial_agreement(db, agreement.id)
            self.assertEqual(unchanged.status, "draft")
            self.assertEqual(unchanged.updated_at, original_updated_at)
            self.assertIsNone(unchanged.generated_pdf_bytes)
            self.assertIsNone(unchanged.generated_pdf_filename)
            self.assertIsNone(unchanged.generated_pdf_sha256)
            self.assertIsNone(unchanged.generated_at)
            self.assertIsNone(unchanged.generated_by_user_id)

    def test_pdf_generation_does_not_query_products_catalog_or_restricted_tables(self) -> None:
        statements: list[str] = []

        def record_statement(_connection, _cursor, statement, _parameters, _context, _many) -> None:
            statements.append(statement.lower())

        with self.Session() as db:
            agreement = self._complete(db)
            event.listen(self.engine, "before_cursor_execute", record_statement)
            try:
                generate_and_store_b2b_commercial_agreement_pdf(
                    db,
                    agreement_id=agreement.id,
                    user_id=None,
                    logo_bytes=None,
                )
            finally:
                event.remove(self.engine, "before_cursor_execute", record_statement)

        joined_sql = "\n".join(statements)
        self.assertNotIn("b2b_customer_products", joined_sql)
        self.assertNotIn(" from products", joined_sql)
        self.assertNotIn("inventory_transactions", joined_sql)
        self.assertNotIn("inventory_balances", joined_sql)
        self.assertNotIn("b2b_sales_orders", joined_sql)
        self.assertNotIn("b2b_ar_payments", joined_sql)
        self.assertNotIn("b2b_ar_opening_balances", joined_sql)

    def test_pdf_builder_works_without_logo_and_filename_is_sanitized(self) -> None:
        self.assertEqual(
            safe_b2b_commercial_agreement_pdf_filename('Ángel / Azul: "Centro"', 0),
            "Acuerdo_Comercial_B2B_Angel_Azul_Centro_v0.pdf",
        )
        with self.Session() as db:
            agreement = self._complete(db)
            pdf_bytes = build_b2b_commercial_agreement_pdf(agreement, logo_bytes=None)
            invalid_logo_pdf = build_b2b_commercial_agreement_pdf(
                agreement,
                logo_bytes=b"not-a-readable-image",
            )
        self.assertTrue(pdf_bytes.startswith(b"%PDF"))
        self.assertGreater(len(pdf_bytes), 5000)
        self.assertTrue(invalid_logo_pdf.startswith(b"%PDF"))

    def test_pdf_service_source_contains_contract_content_but_no_catalog_queries(self) -> None:
        pdf_source = Path(
            "app/services/b2b_commercial_agreement_pdf_service.py"
        ).read_text(encoding="utf-8")
        self.assertIn("ACUERDO COMERCIAL B2B - COMPRA DIRECTA", pdf_source)
        self.assertIn("customer_name_snapshot", pdf_source)
        self.assertIn("render_agreement_template_text", pdf_source)
        self.assertIn("Personas autorizadas para realizar pedidos", pdf_source)
        self.assertIn("Aceptación y firmas", pdf_source)
        self.assertNotIn("B2BCustomerProduct", pdf_source)
        self.assertNotIn("\n    Product,", pdf_source)
        self.assertNotIn("db.query(Product", pdf_source)
        self.assertNotIn("Anexo 1", pdf_source)

    def test_schema_constraints_and_relationships_exist(self) -> None:
        self.assertIn("b2b_commercial_agreements", Base.metadata.tables)
        self.assertIn("b2b_commercial_agreement_people", Base.metadata.tables)
        self.assertIn("b2b_commercial_agreement_sections", Base.metadata.tables)
        with self.Session() as db:
            agreement = self._create(db)
            self.assertEqual(db.query(B2BCommercialAgreement).count(), 1)
            self.assertEqual(db.query(B2BCommercialAgreementSection).count(), 19)
            self.assertEqual(db.query(B2BCommercialAgreementPerson).count(), 0)
            self.assertEqual(agreement.customer.id, self.customer_id)


if __name__ == "__main__":
    unittest.main()
