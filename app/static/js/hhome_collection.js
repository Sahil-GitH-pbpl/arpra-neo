let currentStep = 1;
let linkedPatientsCache = [];
let hasCallerContext = false;
let colonyRequestSeq = 0;
let wizardData = {
  searchedMobile: '',
  appointment: {},
  testsBilling: {}
};
let selectedPatientTags = [];
let editingPatientId = null;
let editingAddressId = null;
let slotPlannerModal = null;
let panelTestsModal = null;
let slotSelectedRoute = '';
let summaryRequestSeq = 0;
let testSpecimenCatalog = null;
let testSpecimenCatalogPromise = null;
let activePanelPicker = {
  patientId: null,
  compCatId: null,
  billingName: '',
  selectedGcode: '',
  selectedScode: '',
  tempSelected: {}
};

const titleGenderMap = {
  Mr: 'Male',
  Mrs: 'Female',
  Ms: 'Female',
  Miss: 'Female',
  Master: 'Male',
  Other: 'Other'
};

function setLayoutForWizard() {
  $('#wizard-right-col').removeClass('d-none');
  $('#wizard-left-col').removeClass('col-lg-12').addClass('col-lg-9');
}

function setLayoutForSuccess() {
  $('#wizard-right-col').addClass('d-none');
  $('#wizard-left-col').removeClass('col-lg-9').addClass('col-lg-12');
}

function setStep(step) {
  setLayoutForWizard();
  slotPlannerModal = null;
  currentStep = step;
  $('.step-pill').removeClass('active');
  $(`.step-pill[data-step="${step}"]`).addClass('active');
  $.get(`/hhome-collection/step/${step}`, function (html) {
    $('#wizard-left-panel').html(html);
    bindStepEvents();
    if (step === 2) hydrateStep2();
    if (step === 3) renderTestsBilling();
    if (step === 4) renderReview();
  });
}

function showAlert(target, type, text) {
  $(target).html(`<div class="alert alert-${type} hc-alert">${text}</div>`);
}

function escHtml(v) {
  return String(v ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function normalizeChargeModeCode(raw) {
  const src = String(raw || '').toUpperCase().trim();
  if (!src) return '';
  const keep = [];
  for (const ch of src) {
    if ((ch === 'C' || ch === 'P' || ch === 'F') && !keep.includes(ch)) {
      keep.push(ch);
    }
  }
  return ['C', 'P', 'F'].filter(ch => keep.includes(ch)).join('');
}

function chargeModeLabel(codeRaw) {
  const code = normalizeChargeModeCode(codeRaw);
  if (!code) return '-';
  const parts = [];
  if (code.includes('C')) parts.push('Credit');
  if (code.includes('P')) parts.push('Paying');
  if (code.includes('F')) parts.push('Free');
  return `${code} (${parts.join(' + ')})`;
}

function chargeModeOptions(codeRaw) {
  const code = normalizeChargeModeCode(codeRaw);
  if (!code) return [];
  return ['C', 'P', 'F'].filter((x) => code.includes(x));
}

function renderChargeModeControl(patientId, billing) {
  const pid = String(patientId || '');
  if (!pid || !billing) return;

  const options = chargeModeOptions(billing.charge_mode_code || billing.charge_mode || '');
  const $holder = $(`#tb-charge-mode-${pid}`);
  if (!$holder.length) return;

  if (!options.length) {
    billing.charge_mode_code = '';
    $holder.text('-');
    return;
  }

  if (options.length === 1) {
    const only = options[0];
    billing.charge_mode_code = only;
    $holder.text(chargeModeLabel(only));
    return;
  }

  let selected = normalizeChargeModeCode(billing.selected_charge_mode || billing.charge_mode_code || billing.charge_mode || '');
  if (!options.includes(selected)) selected = options[0];
  billing.selected_charge_mode = selected;
  billing.charge_mode_code = selected;

  const html = `
    <select id="tb-charge-mode-select-${pid}" class="form-select form-select-sm tb-charge-mode-select" data-patient-id="${escHtml(pid)}">
      ${options.map((m) => `<option value="${escHtml(m)}" ${m === selected ? 'selected' : ''}>${escHtml(chargeModeLabel(m))}</option>`).join('')}
    </select>
  `;
  $holder.html(html);
}

function renderWithTransition(target, html) {
  const $el = $(target);
  $el.removeClass('hc-anim-enter');
  $el.html(html);
  // force reflow so animation retriggers on repeated updates
  void $el[0].offsetWidth;
  $el.addClass('hc-anim-enter');
}

function setCallerInlineChip(caller, notFoundText = '') {
  if (!caller && !notFoundText) {
    $('#caller-inline-chip').addClass('d-none').html('');
    return;
  }
  if (notFoundText) {
    $('#caller-inline-chip')
      .removeClass('d-none')
      .html(`<span class="caller-pill caller-pill-warning">${notFoundText}</span>`);
    return;
  }
  $('#caller-inline-chip')
    .removeClass('d-none')
    .html(`<span class="caller-pill"><strong>${caller.full_name}</strong> <small>(${caller.caller_code})</small></span>`);
}

function renderRightPanelState(patients) {
  const list = patients || [];
  linkedPatientsCache = list;
  if (!list.length) {
    $('#right-panel').html('<div class="text-muted">No linked patients yet</div>');
    return;
  }
  const html = list.map(p => `
    <div class="chip ${p.selected ? 'selected' : ''}" data-patient-id="${p.id}">
      <div><strong>${p.full_name}</strong> (${p.age}) ${renderTagBadges(p.tag)}</div>
      <small>${p.default_address || ''}</small>
    </div>
  `).join('');
  $('#right-panel').html(html);
}

function applyStep1Bundle(res) {
  renderRightPanelState(res.linked_patients || []);
  renderSelectedPatientsState(res.selected_patients || []);
  bindRemovePatientButtons();
  bindEditPatientButtons();
  renderAddressesState(res.addresses || [], res.selected_address_id || 0);
  bindUseAddressButtons();
  bindEditAddressButtons();
}

function bindRemovePatientButtons() {
  $('.rm-patient').off('click').on('click', function () {
    const patientId = $(this).data('patient-id');
    $.ajax({
      url: '/hhome-collection/remove-selected-patient',
      method: 'POST',
      contentType: 'application/json',
      data: JSON.stringify({ patient_id: patientId }),
      success: function (res) {
        applyStep1Bundle(res || {});
      }
    });
  });
}

function bindEditPatientButtons() {
  $('.btn-edit-patient').off('click').on('click', function () {
    const patientId = Number($(this).data('patient-id') || 0);
    if (!patientId) return;
    startEditPatient(patientId);
  });
}

function renderSelectedPatientsState(list) {
  const selectedList = list || [];
  if (!selectedList.length) {
    $('#selected-patient-tags').html('<div class="text-muted">No patients selected yet.</div>');
    return;
  }
  const html = selectedList.map(p => `
    <div class="selected-patient-card">
      <div class="selected-patient-card-top">
        <div class="selected-patient-card-name">${p.full_name} ${renderTagBadges(p.tag)}</div>
        <button class="rm-patient" data-patient-id="${p.patient_id}" title="Remove">x</button>
      </div>
      <div class="selected-patient-card-meta">
        <span><strong>Age:</strong> ${p.age || '-'}</span>
        <span><strong>DOB:</strong> ${p.date_of_birth || '-'}</span>
        <span><strong>Gender:</strong> ${p.gender || '-'}</span>
        <span><strong>Contact:</strong> ${p.contact_mobile || '-'}</span>
        <span><strong>Panel:</strong> ${p.panel_company || '-'}</span>
      </div>
      <div class="selected-patient-card-actions">
        <button class="btn btn-sm btn-outline-success btn-use-address btn-edit-patient" data-patient-id="${p.patient_id}" title="Edit">Edit</button>
      </div>
    </div>
  `).join('');
  $('#selected-patient-tags').html(html);
}

function bindUseAddressButtons() {
  $('#address-list .btn-use-address').off('click').on('click', function () {
    const addressId = $(this).data('address-id');
    if (!addressId) return;
    $.ajax({
      url: '/hhome-collection/select-address',
      method: 'POST',
      contentType: 'application/json',
      data: JSON.stringify({ address_id: addressId }),
      success: function (r) {
        slotSelectedRoute = (r?.snapshot?.route_no_snapshot || '').trim() || slotSelectedRoute;
        $('#slot-selected-route').val(slotSelectedRoute);
        loadAddresses();
      }
    });
  });
}

function bindEditAddressButtons() {
  $('.btn-edit-address').off('click').on('click', function () {
    const addressId = Number($(this).data('address-id') || 0);
    if (!addressId) return;
    startEditAddress(addressId);
  });
}

function renderAddressesState(addresses, selectedAddressId) {
  const list = addresses || [];
  if (!list.length) {
    renderWithTransition('#address-list', '<div class="text-muted">No addresses linked yet.</div>');
    return;
  }
  const selectedId = Number(selectedAddressId || 0);
  const html = list.map(a => `
    <div class="address-card ${selectedId === a.id ? 'selected-address' : ''}">
      <div><strong>${a.address_type}</strong></div>
      <div>${a.house_flat_no}, ${a.floor || ''}, ${a.street_line || ''}</div>
      <div>${a.colony_name_snapshot}, ${a.pincode_snapshot} | ${a.route_no_snapshot} | ${a.city}</div>
      <div class="address-card-actions">
        <button class="btn btn-sm btn-outline-success btn-use-address" data-address-id="${a.id}">Use This Address</button>
        <button class="btn btn-sm btn-outline-success btn-use-address btn-edit-address" data-address-id="${a.id}" title="Edit">Edit</button>
      </div>
    </div>
  `).join('');
  renderWithTransition('#address-list', html);
}

function bindStepEvents() {
  if (currentStep === 1) {
    $('#btn-search-caller').off('click').on('click', searchCaller);
    $('#btn-show-patient-form').off('click').on('click', function () {
      if (editingPatientId) {
        resetPatientFormState();
        return;
      }
      $('#new-patient-form').toggleClass('d-none');
      const opened = !$('#new-patient-form').hasClass('d-none');
      $(this).text(opened ? 'Cancel' : '+ Add New Patient');
      if (!opened) {
        resetPatientFormState();
      }
    });
    $('#btn-save-patient').off('click').on('click', savePatient);
    $('#btn-show-address-form').off('click').on('click', function () {
      if (editingAddressId) {
        resetAddressFormState();
        return;
      }
      $('#new-address-form').toggleClass('d-none');
      const opened = !$('#new-address-form').hasClass('d-none');
      $(this).text(opened ? 'Cancel' : '+ Add New Address');
      if (!opened) {
        resetAddressFormState();
      }
    });
    $('#btn-save-address').off('click').on('click', saveAddress);
    $('#btn-go-step2').off('click').on('click', goStep2);
    $('#p-dob').off('change').on('change', autoFillAgeFromDob);
    $('#p-age-years').off('input change').on('input change', autoDobFromAge);
    $('#p-title').off('change').on('change', autoGenderByTitle);
    initPatientTagPicker();
  wirePatientPanelSuggest();
    $(document).off('change', '#a-city').on('change', '#a-city', function () {
      $('#a-pincode').val('');
      $('#a-route').val('');
      loadColonies(true);
    });
    if ($('#a-city').val()) {
      loadColonies(false);
    }
    resetPatientFormState();
    resetAddressFormState();
  }

  if (currentStep === 2) {
    const today = new Date().toISOString().slice(0, 10);
    $('#b-date').attr('min', today);
    $('#slot-grid-date').attr('min', today);
    wireAppointmentReferredSuggest();
    wireInternalRefSuggest();
    $('#btn-back-step1').off('click').on('click', () => setStep(1));
    $('#btn-go-step3').off('click').on('click', goStep3);
    $('#btn-open-slots').off('click').on('click', openSlotPlanner);
    $('#btn-slot-grid-search').off('click').on('click', loadRouteSlotGrid);
    $('#slot-grid-date').off('change').on('change', loadRouteSlotGrid);
  }

  if (currentStep === 3) {
    $('#btn-back-step2').off('click').on('click', () => setStep(2));
    $('#btn-go-step4').off('click').on('click', goStep4);
    bindPanelBillingEvents();
    $('#btn-apply-panel-tests').off('click').on('click', applySelectedPanelTests);
  }

  if (currentStep === 4) {
    $('#btn-back-step3').off('click').on('click', () => setStep(3));
    $('#btn-confirm-booking').off('click').on('click', confirmBooking);
  }

  $('#right-panel').off('click', '.chip').on('click', '.chip', function () {
    const patientId = $(this).data('patient-id');
    const selected = linkedPatientsCache.find(x => x.id === patientId || x.id === Number(patientId));
    if (selected && selected.selected) {
      $.ajax({
        url: '/hhome-collection/remove-selected-patient',
        method: 'POST',
        contentType: 'application/json',
        data: JSON.stringify({ patient_id: patientId }),
        success: function (res) {
          applyStep1Bundle(res || {});
        }
      });
      return;
    }

    $.ajax({
      url: '/hhome-collection/select-patient',
      method: 'POST',
      contentType: 'application/json',
      data: JSON.stringify({ patient_id: patientId }),
      success: function (res) {
        applyStep1Bundle(res || {});
        if (currentStep === 1) {
          hasCallerContext = true;
          toggleStep1Workspace(true);
        }
      }
    });
  });
}

function renderTagBadges(tagCsv) {
  if (!tagCsv) return '';
  return tagCsv
    .split(',')
    .map(x => x.trim())
    .filter(Boolean)
    .map(t => `<span class="patient-tag-badge">${t}</span>`)
    .join('');
}

function getPatientContactPrefill() {
  const fromSearchState = String(wizardData.searchedMobile || '').trim();
  if (fromSearchState) return fromSearchState;
  return String($('#search-mobile').val() || '').trim();
}

function resetPatientFormState() {
  editingPatientId = null;
  $('#new-patient-form').addClass('d-none');
  $('#btn-save-patient').text('Save Patient');
  $('#btn-show-patient-form').text('+ Add New Patient');
  $('#p-title').val('');
  $('#p-full-name').val('');
  $('#p-labmate-pid').val('');
  $('#p-panel-company').val('');
  $('#p-panel-company-suggest').addClass('d-none').html('');
  $('#p-gender').val('Male');
  $('#p-dob').val('');
  $('#p-age-years').val('');
  $('#p-contact-mobile').val(getPatientContactPrefill());
  $('#p-alternate-mobile').val('');
  $('#p-email').val('');
  selectedPatientTags = [];
  $('#patient-tag-picker .patient-tag-chip').removeClass('active');
}

function resetAddressFormState() {
  editingAddressId = null;
  $('#new-address-form').addClass('d-none');
  $('#btn-save-address').text('Save Address');
  $('#btn-show-address-form').text('+ Add New Address');
  $('#a-type').val('Home');
  $('#a-house').val('');
  $('#a-floor').val('');
  $('#a-city').val('');
  $('#a-colony').val('').trigger('change');
  $('#a-pincode').val('');
  $('#a-route').val('');
  $('#a-street').val('');
  $('#a-access').val('');
}

function initPatientTagPicker() {
  if (!Array.isArray(selectedPatientTags)) selectedPatientTags = [];
  $('#patient-tag-picker .patient-tag-chip').removeClass('active');
  selectedPatientTags.forEach((tag) => {
    $(`#patient-tag-picker .patient-tag-chip[data-tag="${tag}"]`).addClass('active');
  });
  $('#patient-tag-picker').off('click', '.patient-tag-chip').on('click', '.patient-tag-chip', function () {
    const tag = ($(this).data('tag') || '').toString().trim();
    if (!tag) return;
    if ($(this).hasClass('active')) {
      $(this).removeClass('active');
      selectedPatientTags = selectedPatientTags.filter(t => t !== tag);
    } else {
      $(this).addClass('active');
      if (!selectedPatientTags.includes(tag)) selectedPatientTags.push(tag);
    }
  });
}

function autoFillAgeFromDob() {
  const dobStr = $('#p-dob').val();
  if (!dobStr) return;
  const dob = new Date(dobStr + 'T00:00:00');
  if (Number.isNaN(dob.getTime())) return;
  const today = new Date();
  let age = today.getFullYear() - dob.getFullYear();
  const m = today.getMonth() - dob.getMonth();
  if (m < 0 || (m === 0 && today.getDate() < dob.getDate())) {
    age -= 1;
  }
  if (age < 0) age = 0;
  $('#p-age-years').val(age);
}

function autoGenderByTitle() {
  const title = ($('#p-title').val() || '').trim();
  const gender = titleGenderMap[title];
  if (gender) {
    $('#p-gender').val(gender);
  }
}

function autoDobFromAge() {
  const raw = ($('#p-age-years').val() || '').trim();
  if (!raw) return;
  const age = parseInt(raw, 10);
  if (Number.isNaN(age) || age < 0) return;
  const year = new Date().getFullYear() - age;
  const dob = `${year}-01-01`;
  $('#p-dob').val(dob);
}

function toggleStep1Workspace(enabled) {
  const note = enabled
    ? '<span class="text-success">Caller selected. Add/select patient and address.</span>'
    : 'Caller not found. Fill patient form with contact details to auto-create caller.';
  $('#step1-workspace-note').html(note);
}

function searchCaller() {
  const mobile = $('#search-mobile').val().trim();
  if (!mobile) return alert('Enter mobile number');
  wizardData.searchedMobile = mobile;

  $.ajax({
    url: '/hhome-collection/search-caller',
    method: 'POST',
    contentType: 'application/json',
    data: JSON.stringify({ mobile }),
    success: function (res) {
      if (res.found) {
        hasCallerContext = true;
        toggleStep1Workspace(true);
        setCallerInlineChip(res.caller);
        $('#search-result').html('');
        renderSelectedPatientsState(res.selected_patients || []);
        bindRemovePatientButtons();
        bindEditPatientButtons();
        renderAddressesState(res.addresses || [], res.selected_address_id || 0);
        bindUseAddressButtons();
        bindEditAddressButtons();
        renderRightPanelState(res.linked_patients || []);
        resetPatientFormState();
        resetAddressFormState();
      } else {
        hasCallerContext = false;
        setCallerInlineChip(null, 'Caller not found...');
        $('#search-result').html('');
        toggleStep1Workspace(false);
        renderSelectedPatientsState([]);
        renderAddressesState([], 0);
        renderRightPanelState([]);
        resetPatientFormState();
        resetAddressFormState();
        $('#p-contact-mobile').val(res.mobile || mobile);
      }
    },
    error: function (xhr) {
      setCallerInlineChip(null);
      showAlert('#search-result', 'danger', xhr.responseJSON?.message || 'Search failed');
    }
  });
}

function startEditPatient(patientId) {
  $.get(`/hhome-collection/patient/${patientId}`, function (res) {
    const p = res?.patient || {};
    editingPatientId = Number(p.id || patientId);
    $('#new-patient-form').removeClass('d-none');
    $('#btn-save-patient').text('Update Patient');
    $('#btn-show-patient-form').text('Cancel Edit');

    $('#p-title').val(p.title || '');
    $('#p-full-name').val(p.full_name || '');
    $('#p-labmate-pid').val(p.labmate_pid || '');
    $('#p-panel-company').val(p.panel_company || '');
    $('#p-gender').val(p.gender || 'Male');
    $('#p-dob').val(p.date_of_birth || '');
    $('#p-age-years').val(p.age_years || '');
    $('#p-contact-mobile').val(p.contact_mobile || '');
    $('#p-alternate-mobile').val(p.alternate_mobile || '');
    $('#p-email').val(p.email || '');

    selectedPatientTags = (p.tag || '')
      .split(',')
      .map(x => x.trim())
      .filter(Boolean);
    initPatientTagPicker();
  }).fail(function (xhr) {
    alert(xhr.responseJSON?.message || 'Unable to load patient details');
  });
}

function startEditAddress(addressId) {
  $.get(`/hhome-collection/address/${addressId}`, function (res) {
    const a = res?.address || {};
    editingAddressId = Number(a.id || addressId);
    $('#new-address-form').removeClass('d-none');
    $('#btn-save-address').text('Update Address');
    $('#btn-show-address-form').text('Cancel Edit');

    $('#a-type').val(a.address_type || 'Home');
    $('#a-house').val(a.house_flat_no || '');
    $('#a-floor').val(a.floor || '');
    $('#a-city').val(a.city || '');
    $('#a-street').val(a.street_line || '');
    $('#a-access').val(a.access_notes || '');
    $('#a-pincode').val(a.pincode_snapshot || '');
    $('#a-route').val(a.route_no_snapshot || '');

    loadColonies(false, String(a.colony_id || ''));
  }).fail(function (xhr) {
    alert(xhr.responseJSON?.message || 'Unable to load address details');
  });
}


function wirePatientPanelSuggest() {
  const $input = $('#p-panel-company');
  if (!$input.length) return;

  let $suggest = $('#p-panel-company-suggest');
  if (!$suggest.length) {
    $input.after('<div id="p-panel-company-suggest" class="tb-panel-suggest d-none"></div>');
    $suggest = $('#p-panel-company-suggest');
  }

  $input.off('input.patientPanel').on('input.patientPanel', function () {
    const q = String($(this).val() || '').trim();
    if (q.length < 2) {
      $suggest.addClass('d-none').html('');
      return;
    }
    $.get('/hhome-collection/panel-companies', { q, limit: 20, atype: 'C' }, function (res) {
      const items = res?.items || [];
      if (!items.length) {
        $suggest.html('<div class="tb-panel-item">No panel found</div>').removeClass('d-none');
        return;
      }
      const html = items.map(x => `
        <div class="tb-panel-item" data-pname="${escHtml(x.pname || '')}">
          <strong>${escHtml(x.pname || '')}</strong>
          <span class="meta">CenterID: ${escHtml(x.CenterID || '')}</span>
        </div>
      `).join('');
      $suggest.html(html).removeClass('d-none');
    });
  });

  $suggest.off('click.patientPanel', '.tb-panel-item').on('click.patientPanel', '.tb-panel-item', function () {
    const pname = String($(this).data('pname') || '').trim();
    if (pname) $input.val(pname);
    $suggest.addClass('d-none').html('');
  });

  $(document).off('click.patientPanel').on('click.patientPanel', function (e) {
    if (!$(e.target).closest('#p-panel-company, #p-panel-company-suggest').length) {
      $suggest.addClass('d-none').html('');
    }
  });
}

function wireAppointmentReferredSuggest() {
  const $input = $('#ap-referred-by');
  const $suggest = $('#ap-referred-by-suggest');
  if (!$input.length || !$suggest.length) return;

  $input.off('input.appReferred').on('input.appReferred', function () {
    const q = String($(this).val() || '').trim();
    if (q.length < 2) {
      $suggest.addClass('d-none').html('');
      return;
    }
    // Referred-by should include both C and D (no atype filter).
    $.get('/hhome-collection/panel-companies', { q, limit: 20 }, function (res) {
      const items = res?.items || [];
      if (!items.length) {
        $suggest.html('<div class="tb-panel-item">No panel found</div>').removeClass('d-none');
        return;
      }
      const html = items.map(x => `
        <div class="tb-panel-item" data-pname="${escHtml(x.pname || '')}">
          <strong>${escHtml(x.pname || '')}</strong>
          <span class="meta">CenterID: ${escHtml(x.CenterID || '')}</span>
        </div>
      `).join('');
      $suggest.html(html).removeClass('d-none');
    });
  });

  $suggest.off('click.appReferred', '.tb-panel-item').on('click.appReferred', '.tb-panel-item', function () {
    const pname = String($(this).data('pname') || '').trim();
    if (pname) $input.val(pname);
    $suggest.addClass('d-none').html('');
  });

  $(document).off('click.appReferred').on('click.appReferred', function (e) {
    if (!$(e.target).closest('#ap-referred-by, #ap-referred-by-suggest').length) {
      $suggest.addClass('d-none').html('');
    }
  });
}

function wireInternalRefSuggest() {
  const $input = $("#ap-internal-ref");
  const $suggest = $("#ap-internal-ref-suggest");
  if (!$input.length || !$suggest.length) return;

  $input.off("input.appInternalRef").on("input.appInternalRef", function () {
    const q = String($(this).val() || "").trim();
    if (q.length < 2) {
      $suggest.addClass("d-none").html("");
      return;
    }
    $.get('/hhome-collection/internal-ref-users', { q, limit: 20 }, function (res) {
      const items = res?.items || [];
      if (!items.length) {
        $suggest.html('<div class="tb-panel-item">No staff found</div>').removeClass('d-none');
        return;
      }
      const html = items.map(x => `
        <div class="tb-panel-item" data-name="${escHtml(x.name || '')}">
          <strong>${escHtml(x.name || '')}</strong>
        </div>
      `).join('');
      $suggest.html(html).removeClass('d-none');
    });
  });

  $suggest.off('click.appInternalRef', '.tb-panel-item').on('click.appInternalRef', '.tb-panel-item', function () {
    const name = String($(this).data('name') || '').trim();
    if (name) $input.val(name);
    $suggest.addClass('d-none').html('');
  });

  $(document).off('click.appInternalRef').on('click.appInternalRef', function (e) {
    if (!$(e.target).closest('#ap-internal-ref, #ap-internal-ref-suggest').length) {
      $suggest.addClass('d-none').html('');
    }
  });
}
function savePatient() {
  const title = ($('#p-title').val() || '').trim();
  const fullNameCore = ($('#p-full-name').val() || '').trim();

  const data = {
    title: title || null,
    full_name: fullNameCore,
    labmate_pid: $('#p-labmate-pid').val().trim(),
    panel_company: $('#p-panel-company').val().trim(),
    tag: selectedPatientTags.join(','),
    gender: $('#p-gender').val(),
    date_of_birth: $('#p-dob').val() || null,
    age_years: $('#p-age-years').val() || null,
    contact_mobile: $('#p-contact-mobile').val().trim(),
    alternate_mobile: $('#p-alternate-mobile').val().trim(),
    email: $('#p-email').val().trim(),
    searched_mobile: wizardData.searchedMobile
  };

  const isEdit = !!editingPatientId;
  $.ajax({
    url: isEdit ? `/hhome-collection/patient/${editingPatientId}` : '/hhome-collection/create-patient',
    method: isEdit ? 'PATCH' : 'POST',
    contentType: 'application/json',
    data: JSON.stringify(data),
    success: function (res) {
      hasCallerContext = true;
      toggleStep1Workspace(true);
      resetPatientFormState();
      applyStep1Bundle(res || {});
    },
    error: function (xhr) {
      alert(xhr.responseJSON?.message || 'Unable to save patient details');
    }
  });
}

function loadColonies(resetSelection, desiredColonyId = '') {
  const city = $('#a-city').val();
  const requestId = ++colonyRequestSeq;
  const prevSelected = resetSelection ? '' : ($('#a-colony').val() || '');
  $.get('/hhome-collection/colonies', { city: city }, function (res) {
    if (requestId !== colonyRequestSeq) return;
    const options = ['<option value="">Select Colony</option>'];
    (res.colonies || []).forEach(c => {
      options.push(`<option value="${c.id}" data-pincode="${c.pincode}" data-route="${c.route_no}">${c.colony_name}</option>`);
    });
    const $colony = $('#a-colony');
    if ($colony.hasClass('select2-hidden-accessible')) {
      $colony.select2('destroy');
    }
    $colony.html(options.join('')).select2({
      width: '100%',
      dropdownParent: $('#new-address-form')
    });

    $colony.off('select2:open.forceBelow').on('select2:open.forceBelow', function () {
      const api = $(this).data('select2');
      if (!api || !api.$dropdown || !api.$container) return;
      const $dropdown = api.$dropdown;
      const top = api.$container.position().top + api.$container.outerHeight();
      const left = api.$container.position().left;
      $dropdown.css({
        top: `${top}px`,
        left: `${left}px`,
        bottom: 'auto'
      });
      $dropdown.removeClass('select2-dropdown--above').addClass('select2-dropdown--below');
    });

    $colony.off('change').on('change', function () {
      const opt = $(this).find(':selected');
      $('#a-pincode').val(opt.data('pincode') || '');
      $('#a-route').val(opt.data('route') || '');
      const selectedText = opt.text() || 'Select Colony';
      const api = $(this).data('select2');
      if (api && api.$container) {
        api.$container
          .find('.select2-selection__rendered')
          .text(selectedText)
          .attr('title', selectedText);
      }
    });

    if (desiredColonyId && $colony.find(`option[value="${desiredColonyId}"]`).length) {
      $colony.val(desiredColonyId).trigger('change');
    } else if (prevSelected && $colony.find(`option[value="${prevSelected}"]`).length) {
      $colony.val(prevSelected).trigger('change');
    } else if (resetSelection) {
      $colony.val('').trigger('change');
    } else {
      $('#a-pincode').val('');
      $('#a-route').val('');
      $colony.trigger('change');
    }
  });
}

function loadAddresses() {
  $.get('/hhome-collection/addresses', function (res) {
    renderAddressesState(res.addresses || [], res.selected_address_id || 0);
    bindUseAddressButtons();
    bindEditAddressButtons();
  });
}

function saveAddress() {
  if (!hasCallerContext) {
    alert('Save at least one patient first. Caller will auto-create from patient contact.');
    return;
  }
  const data = {
    address_type: $('#a-type').val(),
    house_flat_no: $('#a-house').val().trim(),
    floor: $('#a-floor').val().trim(),
    street_line: $('#a-street').val().trim(),
    landmark: null,
    city: $('#a-city').val(),
    colony_id: $('#a-colony').val(),
    access_notes: $('#a-access').val().trim()
  };

  const isEdit = !!editingAddressId;
  $.ajax({
    url: isEdit ? `/hhome-collection/address/${editingAddressId}` : '/hhome-collection/create-address',
    method: isEdit ? 'PATCH' : 'POST',
    contentType: 'application/json',
    data: JSON.stringify(data),
    success: function () {
      resetAddressFormState();
      loadAddresses();
    },
    error: function (xhr) {
      alert(xhr.responseJSON?.message || 'Unable to save address details');
    }
  });
}

function goStep2() {
  $.get('/hhome-collection/selected-patients', function (pRes) {
    if (!pRes.selected_patients || !pRes.selected_patients.length) {
      alert('Select at least one patient.');
      return;
    }
    $.get('/hhome-collection/addresses', { _ts: Date.now() }, function (aRes) {
      if (!aRes.selected_address_id) {
        alert('Select at least one address (Use This Address).');
        return;
      }
      const line = $('#address-list .address-card.selected-address').find('div').eq(2).text() || '';
      const m = line.match(/\|\s*([^|]+)\s*\|/);
      if (m && m[1]) {
        slotSelectedRoute = m[1].trim();
      }
      const selectedId = Number(aRes.selected_address_id || 0);
      const selectedAddress = (aRes.addresses || []).find(a => Number(a.id) === selectedId) || null;
      if (!slotSelectedRoute) {
        slotSelectedRoute = (selectedAddress?.route_no_snapshot || '').trim();
      }
      setStep(2);
    });
  });
}

function isoTomorrow() {
  const d = new Date();
  d.setDate(d.getDate() + 1);
  return d.toISOString().slice(0, 10);
}

function fetchLatestSelectedRoute(done) {
  const reqId = ++summaryRequestSeq;
  $.get('/hhome-collection/addresses', { _ts: Date.now() }, function (res) {
    if (reqId !== summaryRequestSeq) return;
    const selectedId = Number(res?.selected_address_id || 0);
    const selectedAddress = (res?.addresses || []).find(a => Number(a.id) === selectedId) || null;
    const route = (selectedAddress?.route_no_snapshot || '').trim();
    slotSelectedRoute = route;
    if (typeof done === 'function') done(route);
  }).fail(function () {
    if (reqId !== summaryRequestSeq) return;
    if (typeof done === 'function') done('');
  });
}

function loadTestSpecimenCatalog(done) {
  if (testSpecimenCatalog) {
    done(testSpecimenCatalog);
    return;
  }

  if (testSpecimenCatalogPromise) {
    testSpecimenCatalogPromise
      .done(() => done(testSpecimenCatalog))
      .fail(() => done(null));
    return;
  }

  testSpecimenCatalogPromise = $.get('/hhome-collection/test-specimen-catalog', function (res) {
    if (res?.ok) {
      testSpecimenCatalog = {
        tests: res.tests || {},
        children_by_testcode1: res.children_by_testcode1 || {}
      };
    } else {
      testSpecimenCatalog = { tests: {}, children_by_testcode1: {} };
    }
    done(testSpecimenCatalog);
  }).fail(function () {
    testSpecimenCatalog = { tests: {}, children_by_testcode1: {} };
    done(testSpecimenCatalog);
  });
}

function normalizeTestCode(v) {
  return String(v || '').trim().toUpperCase();
}

function collectTubesForSelectedTest(testItem, catalog) {
  const testsMap = catalog?.tests || {};
  const childMap = catalog?.children_by_testcode1 || {};
  const rootCode = normalizeTestCode(testItem?.testcode1 || testItem?.booked_code || testItem?.test_code || '');
  if (!rootCode) return [];

  const seenNodes = new Set();
  const seenTubes = new Set();
  const tubes = [];

  function pushTube(code) {
    const meta = testsMap[code];
    const tube = String(meta?.specimen_name || '').trim();
    if (!tube) return;
    if (!seenTubes.has(tube)) {
      seenTubes.add(tube);
      tubes.push(tube);
    }
  }

  function dfs(code) {
    if (!code || seenNodes.has(code)) return;
    seenNodes.add(code);
    pushTube(code);
    const children = childMap[code] || [];
    children.forEach((childCode) => dfs(normalizeTestCode(childCode)));
  }

  dfs(rootCode);
  return tubes;
}

function renderReviewTestsHtml(selectedTests, catalog) {
  if (!selectedTests.length) return '-';
  const rows = selectedTests.map((t) => {
    const code = normalizeTestCode(t?.booked_code || t?.testcode1 || t?.test_code || '');
    const desc = String(t?.description || '').trim();
    const label = [code, desc].filter(Boolean).join(' - ') || 'Test';
    const tubes = collectTubesForSelectedTest(t, catalog);
    const tubeText = tubes.length ? tubes.join(', ') : '-';
    return `<li><strong>${escHtml(label)}</strong> (Sample Tube: ${escHtml(tubeText)})</li>`;
  }).join('');
  return `<ol class="mb-0 ps-3">${rows}</ol>`;
}

function hydrateStep2() {
  const defaultDate = isoTomorrow();
  $('#b-date').val(wizardData.appointment.preferred_visit_date || defaultDate);
  $('#b-slot').val(wizardData.appointment.preferred_time_slot || '');
  $('#ap-referred-by').val(wizardData.appointment.referred_by || '');
  $('#ap-internal-ref').val(wizardData.appointment.internal_ref || '');
  $('#b-remarks').val(wizardData.appointment.remarks || '');
  if (slotSelectedRoute) {
    $('#slot-selected-route').val(slotSelectedRoute);
  } else {
    fetchLatestSelectedRoute(function (route) {
      $('#slot-selected-route').val(route);
    });
  }
}

function goStep3() {
  const appt = {
    preferred_visit_date: $('#b-date').val(),
    preferred_time_slot: $('#b-slot').val(),
    referred_by: $('#ap-referred-by').val(),
    internal_ref: $('#ap-internal-ref').val(),
    remarks: $('#b-remarks').val().trim()
  };

  if (!appt.preferred_visit_date || !appt.preferred_time_slot) {
    alert('Preferred Visit Date and Time Slot are required.');
    return;
  }

  wizardData.appointment = appt;
  setStep(3);
}

function formatMinutesTo12h(totalMinutes) {
  const mins = ((totalMinutes % 1440) + 1440) % 1440;
  let h24 = Math.floor(mins / 60);
  const m = mins % 60;
  const ap = h24 >= 12 ? 'PM' : 'AM';
  let h12 = h24 % 12;
  if (h12 === 0) h12 = 12;
  return `${String(h12).padStart(2, '0')}:${String(m).padStart(2, '0')} ${ap}`;
}

function generateHalfHourSlots() {
  const rows = [];
  for (let mins = 6 * 60; mins < 24 * 60; mins += 30) {
    rows.push({
      key: mins,
      label: `${formatMinutesTo12h(mins)} to ${formatMinutesTo12h(mins + 30)}`
    });
  }
  return rows;
}

function openSlotPlanner() {
  const directRoute = ($('#slot-selected-route').val() || slotSelectedRoute || '').trim();
  const openWithRoute = function (route) {
    if (!route) {
      alert('Please select address first so route is available.');
      return;
    }
    const modalEl = document.getElementById('slotPlannerModal');
    slotPlannerModal = new bootstrap.Modal(modalEl);
    $('#slot-selected-route').val(route);
    $('#slot-grid-date').val($('#b-date').val() || isoTomorrow());
    slotPlannerModal.show();
    loadRouteSlotGrid(route);
  };

  if (directRoute) {
    openWithRoute(directRoute);
    return;
  }
  fetchLatestSelectedRoute(openWithRoute);
}

function loadRouteSlotGrid(forcedRoute) {
  const dateVal = $('#slot-grid-date').val() || isoTomorrow();
  const routeVal = (forcedRoute || $('#slot-selected-route').val() || slotSelectedRoute || '').trim();
  $('#slot-grid-wrap').html('<div class="text-muted p-2">Loading slots...</div>');
  $.get('/hhome-collection/route-slot-grid', { date: dateVal, route: routeVal, _ts: Date.now() }, function (res) {
    if (!res.ok) {
      $('#slot-grid-wrap').html(`<div class="text-danger p-2">${res.message || 'Unable to load slots'}</div>`);
      return;
    }
    renderRouteSlotGrid(res);
  }).fail(function (xhr) {
    $('#slot-grid-wrap').html(`<div class="text-danger p-2">${xhr.responseJSON?.message || 'Unable to load slots'}</div>`);
  });
}

function renderRouteSlotGrid(res) {
  const routes = res.routes || [];
  const bookings = res.bookings || [];
  const selectedRoute = (res.selected_route || slotSelectedRoute || '').trim();
  const dateVal = res.date || ($('#slot-grid-date').val() || isoTomorrow());
  $('#slot-grid-meta').text(`Total bookings: ${res.total_bookings || 0}`);

  const indexed = {};
  bookings.forEach(b => {
    const key = `${b.route || 'UNASSIGNED'}|${b.slot_key}`;
    indexed[key] = indexed[key] || [];
    indexed[key].push(b);
  });

  const slots = generateHalfHourSlots();
  let html = '<table class="slot-grid-table"><thead><tr><th class="slot-time-col">Time Slot</th>';
  routes.forEach(r => {
    const cls = r === selectedRoute ? 'route-selected' : '';
    html += `<th class="${cls}">${r}</th>`;
  });
  html += '</tr></thead><tbody>';

  slots.forEach(s => {
    html += `<tr><td class="slot-time-col">${s.label}</td>`;
    routes.forEach(r => {
      const cls = r === selectedRoute ? 'route-selected' : '';
      const items = indexed[`${r}|${s.key}`] || [];
      let cell = '';
      items.forEach(it => {
        const info = [it.city, it.area].filter(Boolean).join(', ');
        const mobile = it.mobile ? ` (${it.mobile})` : '';
        cell += `<span class="slot-book-item">${info}${mobile}</span>`;
      });
      if (r === selectedRoute) {
        cell += `<button class="btn btn-primary slot-pick-btn" data-date="${dateVal}" data-slot="${s.label}">+ Select</button>`;
      }
      html += `<td class="${cls}">${cell || '<span class="text-muted">-</span>'}</td>`;
    });
    html += '</tr>';
  });
  html += '</tbody></table>';

  $('#slot-grid-wrap').html(html);
  $('#slot-grid-wrap .slot-pick-btn').off('click').on('click', function () {
    const pickedDate = $(this).data('date');
    const pickedSlot = $(this).data('slot');
    $('#b-date').val(pickedDate);
    $('#b-slot').val(pickedSlot);
    if (slotPlannerModal) slotPlannerModal.hide();
  });
}

function ensureTbObject(pid) {
  wizardData.testsBilling[pid] = wizardData.testsBilling[pid] || {
    panel: null,
    billing: null,
    selected_tests: []
  };
  return wizardData.testsBilling[pid];
}

function testSelKey(t) {
  const g = String(t?.gcode || '');
  const s = String(t?.scode || '');
  const b = String(t?.booked_code || t?.test_code || '');
  return `${g}|${s}|${b}`.toUpperCase();
}

function autoResolveBillingFromPanel(patientId, panelName) {
  const pid = String(patientId || '');
  const pname = String(panelName || '').trim();
  if (!pid || !pname || pname.length < 2) return;

  const tb = ensureTbObject(pid);
  const existingCompCat = String(tb?.billing?.comp_cat_id || '').trim();
  const existingChargeMode = String(tb?.billing?.charge_mode_code || tb?.billing?.charge_mode || '').trim();
  if (existingCompCat && existingCompCat !== '0' && existingChargeMode) {
    return;
  }

  $.get('/hhome-collection/panel-companies', { q: pname, limit: 20 }, function (res) {
    const items = res.items || [];
    if (!items.length) return;

    const q = pname.toLowerCase();
    let picked = items.find(x => String(x.pname || '').toLowerCase() === q);
    if (!picked) picked = items.find(x => String(x.pname || '').toLowerCase().startsWith(q));
    if (!picked) picked = items[0];
    if (!picked) return;

    tb.panel = {
      center_id: String(picked.CenterID || ''),
      pname: String(picked.pname || '')
    };
    tb.billing = {
      comp_cat_id: String(picked.CompCatID ?? ''),
      cat_details: String(picked.CatDetails || ''),
      charge_mode_code: normalizeChargeModeCode(picked.BillingChargeMode || '')
    };

    $(`#tb-panel-input-${pid}`).val(tb.panel.pname);
    $(`#tb-bill-id-${pid}`).val(tb.billing.comp_cat_id);
    $(`#tb-bill-name-${pid}`).val(tb.billing.cat_details);
    renderChargeModeControl(pid, tb.billing);
    $(`#tb-book-btn-${pid}`).prop('disabled', !String(tb.billing.comp_cat_id || '').trim());
  });
}

function bindPanelBillingEvents() {
  $('#tests-billing-sections').off('input', '.tb-panel-search').on('input', '.tb-panel-search', function () {
    const $input = $(this);
    const patientId = String($input.data('patient-id'));
    const q = ($input.val() || '').trim();
    const $suggest = $(`#tb-panel-suggest-${patientId}`);

    if (q.length < 2) {
      $suggest.addClass('d-none').html('');
      return;
    }

    $.get('/hhome-collection/panel-companies', { q, limit: 20 }, function (res) {
      const items = res.items || [];
      if (!items.length) {
        $suggest.html('<div class="tb-panel-item">No panel found</div>').removeClass('d-none');
        return;
      }
      const rows = items.map(x => `
        <div class="tb-panel-item"
             data-patient-id="${patientId}"
             data-center-id="${escHtml(x.CenterID)}"
             data-pname="${escHtml(x.pname)}"
             data-comp-cat-id="${escHtml(x.CompCatID || '')}"
             data-cat-details="${escHtml(x.CatDetails || '')}"
             data-billing-charge-mode="${escHtml(x.BillingChargeMode || '')}">
          <strong>${escHtml(x.pname)}</strong>
          <span class="meta">CenterID: ${escHtml(x.CenterID)}</span>
        </div>
      `).join('');
      $suggest.html(rows).removeClass('d-none');
    });
  });

  $('#tests-billing-sections').off('click', '.tb-panel-item').on('click', '.tb-panel-item', function () {
    const patientId = String($(this).data('patient-id'));
    const centerId = String($(this).data('center-id') || '');
    const pname = String($(this).data('pname') || '');
    const compCatId = String($(this).data('comp-cat-id') ?? '');
    const catDetails = String($(this).data('cat-details') || '');
    const billingChargeMode = String($(this).data('billing-charge-mode') || '');

    $(`#tb-panel-input-${patientId}`).val(pname);
    $(`#tb-panel-suggest-${patientId}`).addClass('d-none').html('');

    const tb = ensureTbObject(patientId);
    tb.panel = { center_id: centerId, pname };
    tb.billing = {
      comp_cat_id: compCatId,
      cat_details: catDetails,
      charge_mode_code: normalizeChargeModeCode(billingChargeMode)
    };
    tb.selected_tests = [];
    renderSelectedTestsForPatient(patientId);
    $(`#tb-bill-id-${patientId}`).val(tb.billing.comp_cat_id);
    $(`#tb-bill-name-${patientId}`).val(tb.billing.cat_details);
    renderChargeModeControl(patientId, tb.billing);
    $(`#tb-book-btn-${patientId}`).prop('disabled', !String(tb.billing.comp_cat_id || '').trim());
  });

  $(document).off('click.hcPanelClose').on('click.hcPanelClose', function (e) {
    if (!$(e.target).closest('.tb-panel-wrap').length) {
      $('.tb-panel-suggest').addClass('d-none').html('');
    }
  });

  $('#tests-billing-sections').off('change', '.tb-charge-mode-select').on('change', '.tb-charge-mode-select', function () {
    const pid = String($(this).data('patient-id') || '');
    if (!pid) return;
    const tb = ensureTbObject(pid);
    tb.billing = tb.billing || {};
    const selected = normalizeChargeModeCode($(this).val() || '');
    tb.billing.selected_charge_mode = selected;
    tb.billing.charge_mode_code = selected;
  });

  $('#tests-billing-sections').off('click', '.tb-open-panel-tests').on('click', '.tb-open-panel-tests', function () {
    const patientId = String($(this).data('patient-id'));
    openPanelTestsModal(patientId);
  });

  $('#tests-billing-sections').off('click', '.tb-remove-selected-test').on('click', '.tb-remove-selected-test', function () {
    const pid = String($(this).data('patient-id') || '');
    const gcode = String($(this).data('gcode') || '');
    const scode = String($(this).data('scode') || '');
    const booked = String($(this).data('booked-code') || '');
    if (!pid || !booked) return;

    const tb = ensureTbObject(pid);
    const selected = tb.selected_tests || [];
    tb.selected_tests = selected.filter((t) => {
      const sameG = String(t.gcode || '') === gcode;
      const sameS = String(t.scode || '') === scode;
      const sameB = String(t.booked_code || t.test_code || '') === booked;
      return !(sameG && sameS && sameB);
    });
    renderSelectedTestsForPatient(pid);
  });
}

function renderSelectedTestsForPatient(patientId) {
  const pid = String(patientId);
  const tb = ensureTbObject(pid);
  const selected = tb.selected_tests || [];
  $(`#tb-selected-count-${pid}`).text(selected.length);
  if (!selected.length) {
    $(`#tb-selected-list-${pid}`).html('<div class="text-muted">No tests selected yet</div>');
    return;
  }
  const html = selected.map((t) => {
    const code = (t.booked_code || t.test_code || '').toString().trim();
    const desc = (t.description || '').toString().trim();
    const label = [code, desc].filter(Boolean).join(' - ');
    return `
      <span class="badge text-bg-light border me-1 mb-1 d-inline-flex align-items-center gap-1">
        ${escHtml(label || 'Test')}
        <button
          type="button"
          class="tb-remove-selected-test"
          aria-label="Remove"
          data-patient-id="${escHtml(pid)}"
          data-gcode="${escHtml(String(t.gcode || ''))}"
          data-scode="${escHtml(String(t.scode || ''))}"
          data-booked-code="${escHtml(code)}"
        >×</button>
      </span>
    `;
  }).join('');
  $(`#tb-selected-list-${pid}`).html(html);
}

function openPanelTestsModal(patientId) {
  const pid = String(patientId);
  const tb = ensureTbObject(pid);
  const compCatId = tb.billing?.comp_cat_id || '';
  if (!compCatId) {
    alert('Panel company select karke billing category load karein, phir Book Test karein.');
    return;
  }

  activePanelPicker = {
    patientId: pid,
    compCatId,
    billingName: tb.billing?.cat_details || '',
    selectedGcode: '',
    selectedScode: '',
    tempSelected: (tb.selected_tests || []).reduce((acc, t) => {
      acc[testSelKey(t)] = t;
      return acc;
    }, {})
  };

  const patientName = $(`#tb-patient-name-${pid}`).text() || `Patient ${pid}`;
  $('#panel-modal-meta').text(`Patient: ${patientName} | CompCatID: ${compCatId} | ${activePanelPicker.billingName}`);

  $('#panel-groups-list').html('<div class="text-muted p-2">Loading groups...</div>');
  $('#panel-subgroups-list').html('<div class="text-muted p-2">Select group</div>');
  $('#panel-tests-list').html('<div class="text-muted p-2">Select subgroup</div>');
  $('#panel-child-tests-list').html('<div class="text-muted p-2">Child Tests button se list khulegi</div>');

  const modalEl = document.getElementById('panelTestsModal');
  panelTestsModal = new bootstrap.Modal(modalEl);
  panelTestsModal.show();
  loadPanelGroups();
}

function loadPanelGroups() {
  $.get('/hhome-collection/panel-groups', { comp_cat_id: activePanelPicker.compCatId }, function (res) {
    const groups = res.groups || [];
    if (!groups.length) {
      $('#panel-groups-list').html('<div class="text-muted p-2">No groups mapped</div>');
      return;
    }
    const html = groups.map(g => `
      <div class="panel-row-item panel-group-item" data-gcode="${escHtml(g.gcode)}">
        <span class="panel-row-code">${escHtml(g.gcode)}</span>${escHtml(g.description || '')}
      </div>
    `).join('');
    $('#panel-groups-list').html(html);

    $('#panel-groups-list .panel-group-item').off('click').on('click', function () {
      $('#panel-groups-list .panel-row-item').removeClass('active');
      $(this).addClass('active');
      activePanelPicker.selectedGcode = String($(this).data('gcode') || '');
      activePanelPicker.selectedScode = '';
      loadPanelSubgroups();
    });

    const first = groups[0];
    if (first && first.gcode) {
      activePanelPicker.selectedGcode = String(first.gcode);
      $('#panel-groups-list .panel-group-item').first().addClass('active');
      loadPanelSubgroups();
    }
  });
}

function loadPanelSubgroups() {
  $('#panel-subgroups-list').html('<div class="text-muted p-2">Loading subgroups...</div>');
  $('#panel-tests-list').html('<div class="text-muted p-2">Select subgroup</div>');
  $('#panel-child-tests-list').html('<div class="text-muted p-2">Child Tests button se list khulegi</div>');
  $.get('/hhome-collection/panel-subgroups', {
    comp_cat_id: activePanelPicker.compCatId,
    gcode: activePanelPicker.selectedGcode
  }, function (res) {
    const subgroups = res.subgroups || [];
    if (!subgroups.length) {
      $('#panel-subgroups-list').html('<div class="text-muted p-2">No subgroups mapped</div>');
      return;
    }
    const html = subgroups.map(s => `
      <div class="panel-row-item panel-subgroup-item" data-scode="${escHtml(s.scode)}">
        <span class="panel-row-code">${escHtml(s.scode)}</span>${escHtml(s.description || '')}
      </div>
    `).join('');
    $('#panel-subgroups-list').html(html);

    $('#panel-subgroups-list .panel-subgroup-item').off('click').on('click', function () {
      $('#panel-subgroups-list .panel-row-item').removeClass('active');
      $(this).addClass('active');
      activePanelPicker.selectedScode = String($(this).data('scode') || '');
      loadPanelTests();
    });

    const first = subgroups[0];
    if (first && first.scode) {
      activePanelPicker.selectedScode = String(first.scode);
      $('#panel-subgroups-list .panel-subgroup-item').first().addClass('active');
      loadPanelTests();
    }
  });
}

function loadPanelTests() {
  $('#panel-tests-list').html('<div class="text-muted p-2">Loading tests...</div>');
  $('#panel-child-tests-list').html('<div class="text-muted p-2">Child Tests button se list khulegi</div>');
  $.get('/hhome-collection/panel-tests', {
    comp_cat_id: activePanelPicker.compCatId,
    gcode: activePanelPicker.selectedGcode,
    scode: activePanelPicker.selectedScode
  }, function (res) {
    const tests = res.tests || [];
    if (!tests.length) {
      $('#panel-tests-list').html('<div class="text-muted p-2">No tests mapped</div>');
      return;
    }

    const modalSelected = activePanelPicker.tempSelected || {};

    const html = tests.map(t => {
      const key = testSelKey(t);
      const checked = modalSelected[key] ? 'checked' : '';
      const childBtn = t.has_children
        ? `<button type="button" class="panel-child-btn"
              data-parent-gcode="${escHtml(t.gcode)}"
              data-parent-scode="${escHtml(t.scode)}"
              data-parent-test-code="${escHtml(t.test_code || '')}">
              Child Tests
           </button>`
        : '';
      return `
        <label class="panel-test-item">
          <input type="checkbox" class="panel-test-check" ${checked}
            data-gcode="${escHtml(t.gcode)}"
            data-scode="${escHtml(t.scode)}"
            data-test-code="${escHtml(t.test_code || '')}"
            data-testcode1="${escHtml(t.testcode1 || '')}"
            data-booked-code="${escHtml(t.booked_code || '')}"
            data-description="${escHtml(t.description || '')}"
            data-charge="${escHtml(t.charge || '')}"
            data-mrp="${escHtml(t.mrp || '')}"
            data-max-discount="${escHtml(t.max_discount || '')}"
          />
            <div class="panel-test-main">
              <div><strong>${escHtml(t.booked_code || '')}</strong> - ${escHtml(t.description || '')}</div>
              <div class="panel-test-meta">Charge: ${escHtml(t.charge || 0)} | MRP: ${escHtml(t.mrp || 0)} | MaxDisc: ${escHtml(t.max_discount || 0)}</div>
            </div>
            ${childBtn ? `<div class="panel-test-actions">${childBtn}</div>` : ''}
          </label>
      `;
    }).join('');

    $('#panel-tests-list').html(html);

    $('#panel-tests-list .panel-test-check').off('change').on('change', function () {
      const pick = {
        gcode: String($(this).data('gcode') || ''),
        scode: String($(this).data('scode') || ''),
        test_code: String($(this).data('test-code') || ''),
        testcode1: String($(this).data('testcode1') || ''),
        booked_code: String($(this).data('booked-code') || ''),
        description: String($(this).data('description') || ''),
        charge: Number($(this).data('charge') || 0),
        mrp: Number($(this).data('mrp') || 0),
        max_discount: Number($(this).data('max-discount') || 0)
      };
      const key = testSelKey(pick);
      activePanelPicker.tempSelected = activePanelPicker.tempSelected || {};
      if ($(this).is(':checked')) {
        activePanelPicker.tempSelected[key] = pick;
      } else {
        delete activePanelPicker.tempSelected[key];
      }
    });

    $('#panel-tests-list .panel-child-btn').off('click').on('click', function (e) {
      e.preventDefault();
      e.stopPropagation();
      const parentGcode = String($(this).data('parent-gcode') || '');
      const parentScode = String($(this).data('parent-scode') || '');
      const parentTestCode = String($(this).data('parent-test-code') || '');
      loadPanelChildTests(parentGcode, parentScode, parentTestCode);
    });
  });
}

function loadPanelChildTests(parentGcode, parentScode, parentTestCode) {
  if (!parentGcode || !parentScode || !parentTestCode) {
    $('#panel-child-tests-list').html('<div class="text-muted p-2">No child mapping</div>');
    return;
  }

  $('#panel-child-tests-list').html('<div class="text-muted p-2">Loading child tests...</div>');

  $.get('/hhome-collection/panel-child-tests', {
    parent_gcode: parentGcode,
    parent_scode: parentScode,
    parent_test_code: parentTestCode
  }, function (res) {
    const tests = res.tests || [];
    if (!tests.length) {
      $('#panel-child-tests-list').html('<div class="text-muted p-2">No child tests found</div>');
      return;
    }

    const rows = tests.map(t => {
      const childBtn = t.has_children
        ? `<button type="button" class="panel-child-next-btn"
              data-parent-gcode="${escHtml(t.gcode)}"
              data-parent-scode="${escHtml(t.scode)}"
              data-parent-test-code="${escHtml(t.test_code || '')}">
              Child Tests
           </button>`
        : '';
      return `
        <div class="panel-child-item">
          <div class="panel-child-line"><strong>${escHtml(t.booked_code || '')}</strong> - ${escHtml(t.description || '')}</div>
          ${childBtn}
        </div>
      `;
    }).join('');

    $('#panel-child-tests-list').html(rows);

    $('#panel-child-tests-list .panel-child-next-btn').off('click').on('click', function () {
      const g = String($(this).data('parent-gcode') || '');
      const s = String($(this).data('parent-scode') || '');
      const tc = String($(this).data('parent-test-code') || '');
      loadPanelChildTests(g, s, tc);
    });
  }).fail(function () {
    $('#panel-child-tests-list').html('<div class="text-danger p-2">Child tests load failed</div>');
  });
}

function applySelectedPanelTests() {
  const pid = String(activePanelPicker.patientId || '');
  if (!pid) return;
  const tb = ensureTbObject(pid);
  tb.selected_tests = Object.values(activePanelPicker.tempSelected || {});
  renderSelectedTestsForPatient(pid);
  if (panelTestsModal) panelTestsModal.hide();
}

function renderTestsBilling() {
  $.get('/hhome-collection/selected-patients', function (res) {
    const list = res.selected_patients || [];
    if (!list.length) {
      $('#tests-billing-sections').html('<div class="alert alert-warning">No patients selected.</div>');
      return;
    }

    const html = list.map(p => {
      const pid = String(p.patient_id);
      const existing = ensureTbObject(pid);
      if ((!existing.panel || !existing.panel.pname) && p.panel_company) {
        existing.panel = existing.panel || {};
        existing.panel.pname = p.panel_company;
      }
      const panel = existing.panel || {};
      const billing = existing.billing || {};
      const chargeModeDisplay = chargeModeLabel(billing.charge_mode_code || billing.charge_mode || '');
      const selectedCount = (existing.selected_tests || []).length;
      return `
      <div class="card mb-2">
        <div class="card-body">
          <h6 id="tb-patient-name-${pid}">${escHtml(p.full_name || '')}</h6>
          <div class="row g-2">
            <div class="col-md-4 tb-panel-wrap">
              <label class="form-label">Panel Company</label>
              <input id="tb-panel-input-${pid}" class="form-control tb-panel-search" data-patient-id="${pid}" value="${escHtml(panel.pname || '')}" placeholder="Type panel company (min 2 chars)">
              <div id="tb-panel-suggest-${pid}" class="tb-panel-suggest d-none"></div>
            </div>
            <div class="col-md-3">
              <label class="form-label">Billing Category ID</label>
              <input id="tb-bill-id-${pid}" class="form-control" value="${escHtml(billing.comp_cat_id || '')}" readonly>
            </div>
            <div class="col-md-3">
              <label class="form-label">Billing Category</label>
              <input id="tb-bill-name-${pid}" class="form-control" value="${escHtml(billing.cat_details || '')}" readonly>
            </div>
            <div class="col-md-2">
              <label class="form-label">Charge Mode</label>
              <div id="tb-charge-mode-${pid}" class="tb-charge-mode-view">${escHtml(chargeModeDisplay)}</div>
            </div>

            <div class="col-md-12 d-flex align-items-center justify-content-between">
              <div class="small text-muted">Selected tests: <strong id="tb-selected-count-${pid}">${selectedCount}</strong></div>
              <button id="tb-book-btn-${pid}" class="btn btn-dark btn-sm tb-open-panel-tests" data-patient-id="${pid}" ${String(billing.comp_cat_id ?? '').trim() ? '' : 'disabled'}>Book Test</button>
            </div>
            <div class="col-12">
              <div id="tb-selected-list-${pid}" class="tb-tests-readonly"></div>
            </div>
          </div>
        </div>
      </div>`;
    }).join('');

    $('#tests-billing-sections').html(html);
    list.forEach(p => {
      const pid = String(p.patient_id);
      renderSelectedTestsForPatient(pid);
      const tb = ensureTbObject(pid);
      if (tb.billing) renderChargeModeControl(pid, tb.billing);
    });
    list.forEach(p => autoResolveBillingFromPanel(String(p.patient_id), p.panel_company || ''));
  });
}

function goStep4() {
  const tb = wizardData.testsBilling || {};

  const missingPanel = Object.keys(tb).find(pid => !tb[pid]?.panel?.pname || !tb[pid]?.billing?.comp_cat_id);
  if (missingPanel) {
    alert('Har selected patient ke liye panel company select karke billing category load karna required hai.');
    return;
  }

  wizardData.testsBilling = tb;
  setStep(4);
}

function renderReview() {
  $.get('/hhome-collection/summary', function (res) {
    if (!res.ok) {
      $('#booking-summary').html('Summary not available');
      return;
    }

    loadTestSpecimenCatalog(function (catalog) {
      const caller = res.caller || {};
      const patients = res.selected_patients || [];
      const addr = res.selected_address || {};
      const ap = wizardData.appointment || {};

      $('#booking-summary').html(`
        <strong>Caller:</strong> ${caller.full_name || '-'} | 
        <strong>Patients:</strong> ${patients.length} | 
        <strong>Address:</strong> ${addr.house_flat_no || ''}, ${addr.colony_name_snapshot || ''}<br>
        <strong>Appointment:</strong> ${ap.preferred_visit_date || '-'} ${ap.preferred_time_slot || '-'}
      `);

      const rows = patients.map(p => {
        const tb = wizardData.testsBilling[p.patient_id] || wizardData.testsBilling[String(p.patient_id)] || {};
        const panelName = tb?.panel?.pname || '-';
        const billing = tb?.billing || {};
        const selectedTests = tb?.selected_tests || [];
        const testCount = selectedTests.length;
        const testsHtml = renderReviewTestsHtml(selectedTests, catalog);
        return `
        <div class="card mb-2">
          <div class="card-body">
            <h6>${p.full_name}</h6>
            <div><strong>Panel:</strong> ${panelName}</div>
            <div><strong>Billing Category:</strong> ${billing.comp_cat_id || '-'} ${billing.cat_details ? `(${billing.cat_details})` : ''}</div>
            <div><strong>Tests (${testCount}):</strong> ${testsHtml}</div>
          </div>
        </div>`;
      }).join('');

      $('#review-patient-sections').html(rows || '<div class="text-muted">No patient data.</div>');
    });
  });
}

function confirmBooking() {
  $.get('/hhome-collection/selected-patients', function (res) {
    const testsMetaMap = {};
    (res.selected_patients || []).forEach(p => {
      const tb = wizardData.testsBilling[p.patient_id] || wizardData.testsBilling[String(p.patient_id)] || {};
      testsMetaMap[p.patient_id] = {
        panel: tb.panel || null,
        billing: tb.billing || null,
        selected_tests: tb.selected_tests || []
      };
    });

    const payload = {
      preferred_visit_date: wizardData.appointment.preferred_visit_date,
      preferred_time_slot: wizardData.appointment.preferred_time_slot,
      special_instructions: `Referred By: ${wizardData.appointment.referred_by || ''}, Internal Ref: ${wizardData.appointment.internal_ref || ''}`,
      remarks: wizardData.appointment.remarks || '',
      patient_tests_meta_map: testsMetaMap
    };

    $.ajax({
      url: '/hhome-collection/confirm-booking',
      method: 'POST',
      contentType: 'application/json',
      data: JSON.stringify(payload),
      success: function (r) {
        $.get(`/hhome-collection/success?booking_id=${r.booking_id}`, function (html) {
          $('#wizard-left-panel').html(html);
          setLayoutForSuccess();
          $('#right-panel').html('');
          $('.step-pill').removeClass('active');
          wizardData = { appointment: {}, testsBilling: {} };
        });
      },
      error: function (xhr) {
        alert(xhr.responseJSON?.message || 'Booking failed');
      }
    });
  });
}

$(function () {
  if (!$('#wizard-left-panel').length) return;
  setStep(1);
  renderRightPanelState([]);

  $(document).on('click', '.step-pill', function () {
    const step = Number($(this).data('step'));
    if (step >= 1 && step <= 4) setStep(step);
  });
});




