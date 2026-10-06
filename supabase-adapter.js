(function () {
  'use strict';

  const SUPABASE_URL = 'https://zkgclcthbubqhqwbjrhy.supabase.co';
  const SUPABASE_KEY = 'sb_publishable_xmFLVtt8PGhCNAtn9uDiGQ_5RRbnIkC';

  if (!window.supabase || !window.supabase.createClient) {
    throw new Error('Supabase JS não carregado.');
  }

  const client = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY, {
    auth: {
      persistSession: true,
      autoRefreshToken: true,
      detectSessionInUrl: true
    }
  });

  const clone = v => JSON.parse(JSON.stringify(v == null ? {} : v));
  const emailNorm = v => String(v || '').trim().toLowerCase();
  const makeId = () => {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    return Math.random().toString(36).slice(2) + Date.now().toString(36);
  };

  function mapError(error) {
    const e = new Error((error && error.message) || 'Erro no banco.');
    e.code = (error && (error.code || error.status)) || 'db_error';
    return e;
  }

  function docSnapshot(row) {
    return {
      id: row.id,
      exists: true,
      data: () => clone(row.data || {})
    };
  }

  function querySnapshot(rows) {
    return { docs: (rows || []).map(docSnapshot) };
  }

  async function fetchRows(collection, where) {
    let q = client.from('documents').select('id,data').eq('collection', collection);
    if (where) {
      const [field, op, value] = where;
      if (op !== '==') throw new Error('Operador não suportado: ' + op);
      q = q.eq('data->>' + field, String(value));
    }
    const { data, error } = await q;
    if (error) throw mapError(error);
    return data || [];
  }

  function subscribe(collection, where, next, errorCb) {
    let alive = true;
    let timer = null;
    const emit = async () => {
      clearTimeout(timer);
      timer = setTimeout(async () => {
        if (!alive) return;
        try {
          const rows = await fetchRows(collection, where);
          if (alive) next(querySnapshot(rows));
        } catch (e) {
          if (alive && errorCb) errorCb(e);
        }
      }, 30);
    };

    emit();
    const channel = client
      .channel('docs-' + collection + '-' + makeId())
      .on(
        'postgres_changes',
        { event: '*', schema: 'public', table: 'documents', filter: 'collection=eq.' + collection },
        emit
      )
      .subscribe();

    return () => {
      alive = false;
      clearTimeout(timer);
      client.removeChannel(channel);
    };
  }

  function documentRef(collection, id) {
    return {
      id,
      async get() {
        const { data, error } = await client
          .from('documents')
          .select('id,data')
          .eq('collection', collection)
          .eq('id', id)
          .maybeSingle();
        if (error) throw mapError(error);
        if (!data) return { id, exists: false, data: () => undefined };
        return docSnapshot(data);
      },
      async set(value) {
        const { error } = await client.from('documents').upsert({
          collection,
          id,
          data: clone(value),
          updated_at: new Date().toISOString()
        }, { onConflict: 'collection,id' });
        if (error) throw mapError(error);
      },
      async update(patch) {
        const { error } = await client.rpc('merge_document', {
          p_collection: collection,
          p_id: id,
          p_patch: clone(patch)
        });
        if (error) throw mapError(error);
      },
      async delete() {
        const { error } = await client
          .from('documents')
          .delete()
          .eq('collection', collection)
          .eq('id', id);
        if (error) throw mapError(error);
      },
      onSnapshot(next, errorCb) {
        let alive = true;
        let timer = null;
        const emit = async () => {
          clearTimeout(timer);
          timer = setTimeout(async () => {
            if (!alive) return;
            try {
              const snap = await this.get();
              if (alive) next(snap);
            } catch (e) {
              if (alive && errorCb) errorCb(e);
            }
          }, 30);
        };
        emit();
        const channel = client
          .channel('doc-' + collection + '-' + id + '-' + makeId())
          .on(
            'postgres_changes',
            { event: '*', schema: 'public', table: 'documents', filter: 'collection=eq.' + collection },
            payload => {
              const row = payload.new && payload.new.id ? payload.new : payload.old;
              if (!row || row.id === id) emit();
            }
          )
          .subscribe();
        return () => {
          alive = false;
          clearTimeout(timer);
          client.removeChannel(channel);
        };
      }
    };
  }

  function collectionRef(name) {
    return {
      doc(id) {
        return documentRef(name, id || makeId());
      },
      async add(value) {
        const ref = documentRef(name, makeId());
        await ref.set(value);
        return ref;
      },
      where(field, op, value) {
        const where = [field, op, value];
        return {
          async get() {
            return querySnapshot(await fetchRows(name, where));
          },
          onSnapshot(next, err) {
            return subscribe(name, where, next, err);
          }
        };
      },
      onSnapshot(next, err) {
        return subscribe(name, null, next, err);
      }
    };
  }

  const db = {
    collection: collectionRef,
    doc(path) {
      const i = String(path).indexOf('/');
      if (i < 1) throw new Error('Caminho inválido: ' + path);
      return documentRef(path.slice(0, i), path.slice(i + 1));
    }
  };

  async function accessFor(email) {
    const { data, error } = await client
      .from('app_users')
      .select('email,role,active,auth_user_id')
      .ilike('email', emailNorm(email))
      .maybeSingle();
    if (error) throw mapError(error);
    return data || null;
  }

  async function syncAccess(email, role, active) {
    email = emailNorm(email);
    if (!email) return;
    const { error } = await client.from('app_users').upsert({
      email,
      role: role === 'admin' ? 'admin' : role === 'gestor' ? 'gestor' : 'colaborador',
      active: active !== false,
      updated_at: new Date().toISOString()
    }, { onConflict: 'email' });
    if (error) throw mapError(error);
  }

  async function removeAccess(email) {
    email = emailNorm(email);
    if (!email) return;
    const { error } = await client.from('app_users').delete().eq('email', email);
    if (error) throw mapError(error);
  }

  async function linkPerson(personId) {
    const { data, error } = await client.rpc('link_person_self', { p_person_id: personId });
    if (error) throw mapError(error);
    return !!data;
  }

  const userAdapter = {
    async id() {
      const { data } = await client.auth.getUser();
      return data && data.user ? data.user.id : null;
    },
    async isOwner() {
      const { data } = await client.auth.getUser();
      const email = data && data.user && data.user.email;
      if (!email) return false;
      const a = await accessFor(email);
      return !!a && a.active !== false && a.role === 'admin';
    },
    async profiles(ids) {
      const out = {};
      if (!Array.isArray(ids) || !ids.length) return out;
      const { data, error } = await client
        .from('app_users')
        .select('auth_user_id,email')
        .in('auth_user_id', ids);
      if (!error) (data || []).forEach(x => {
        if (x.auth_user_id) out[x.auth_user_id] = { name: x.email };
      });
      return out;
    }
  };

  function injectAuthUI() {
    if (document.getElementById('authGate')) return;
    const style = document.createElement('style');
    style.textContent = `
      .auth-gate{
        min-height:100vh;
        display:flex;
        align-items:center;
        justify-content:center;
        padding:24px;
        background:
          radial-gradient(circle at 8% 8%,rgba(145,93,235,.14),transparent 28%),
          radial-gradient(circle at 92% 92%,rgba(124,76,224,.10),transparent 30%),
          linear-gradient(180deg,#fbfaff 0%,#f4f0ff 100%);
        font-family:"Inter",system-ui,-apple-system,"Segoe UI",sans-serif;
        color:#18143f;
      }
      .auth-card{
        width:min(382px,100%);
        background:#fff;
        border:1px solid #e7e1f2;
        border-radius:16px;
        padding:24px;
        box-shadow:0 18px 46px rgba(69,45,121,.12),0 4px 12px rgba(69,45,121,.05);
      }
      .auth-brand{
        font-size:12px;
        font-weight:700;
        letter-spacing:.08em;
        text-transform:uppercase;
        color:#7c4ce0;
        text-align:center;
        margin-bottom:6px;
      }
      .auth-card h1.form-title{
        margin:0 0 18px;
        font-size:22px;
        line-height:1.35;
        font-weight:800;
        text-align:center;
        color:#18143f;
        letter-spacing:-.025em;
      }
      .auth-form{display:block}
      .input-container{
        position:relative;
        margin:10px 0;
      }
      .input-container input{
        width:100%;
        min-height:48px;
        outline:none;
        border:1px solid #e5e0ef;
        background:#fff;
        padding:12px 44px 12px 14px;
        font:inherit;
        font-size:14px;
        line-height:1.25rem;
        color:#241b50;
        border-radius:10px;
        box-shadow:0 1px 2px rgba(32,24,76,.04);
        transition:border-color .2s ease,box-shadow .2s ease;
        box-sizing:border-box;
      }
      .input-container input::placeholder{color:#a19bb5}
      .input-container input:focus{
        border-color:#9b79e4;
        box-shadow:0 0 0 3px rgba(124,76,224,.10),0 1px 2px rgba(32,24,76,.04);
      }
      .input-container span{
        display:grid;
        position:absolute;
        top:0;
        bottom:0;
        right:0;
        width:42px;
        place-content:center;
        pointer-events:none;
      }
      .input-container span svg{
        color:#9b92b4;
        width:17px;
        height:17px;
      }
      .auth-submit{
        display:block;
        width:100%;
        min-height:46px;
        margin:14px 0 0;
        border:0;
        border-radius:10px;
        background:linear-gradient(135deg,#8e5ce8,#7240d4);
        color:#fff;
        font-size:13px;
        line-height:1.25rem;
        font-weight:800;
        letter-spacing:.035em;
        text-transform:uppercase;
        cursor:pointer;
        box-shadow:0 8px 18px rgba(124,76,224,.20);
        transition:transform .16s ease,box-shadow .2s ease,background .2s ease;
      }
      .auth-submit:hover{
        background:linear-gradient(135deg,#8250de,#6431c4);
        box-shadow:0 10px 22px rgba(124,76,224,.26);
        transform:translateY(-1px);
      }
      .auth-submit:active{transform:translateY(0) scale(.99)}
      .auth-submit:disabled,.signup-action:disabled{opacity:.55;cursor:not-allowed}
      .signup-link{
        margin:16px 0 0;
        color:#78718f;
        font-size:13px;
        line-height:1.35;
        text-align:center;
      }
      .signup-action{
        all:unset;
        color:#7440d8;
        font-weight:700;
        text-decoration:underline;
        text-underline-offset:2px;
        cursor:pointer;
      }
      .signup-action:hover{color:#5e2dbd}
      .auth-msg{
        min-height:20px;
        margin-top:12px;
        font-size:12.5px;
        text-align:center;
        color:#77718f;
        line-height:1.4;
      }
      .auth-msg.err{color:#c23d54}
      .auth-msg.ok{color:#16844b}
      .auth-denied{text-align:center}
      .auth-denied strong{display:block;font-size:18px;margin-bottom:8px;color:#18143f}
      .auth-denied p{color:#77718f;line-height:1.5}
      .auth-btn{
        border:1px solid #dfd7ed;
        background:#fff;
        color:#33295d;
        border-radius:9px;
        padding:9px 13px;
        font-weight:700;
        cursor:pointer;
      }
      @media(max-width:520px){
        .auth-gate{padding:16px}
        .auth-card{padding:20px;border-radius:14px}
      }
    `;    document.head.appendChild(style);

    const gate = document.createElement('div');
    gate.id = 'authGate';
    gate.className = 'auth-gate';
    gate.hidden = true;
    gate.innerHTML = `
      <div class="auth-card">
        <div class="auth-brand">Atlas</div>
        <form class="auth-form" id="authForm">
          <p class="form-title">Acesse sua conta</p>
          <div class="input-container">
            <input id="authEmail" placeholder="Digite seu e-mail" type="email" autocomplete="email" required>
            <span aria-hidden="true">
              <svg stroke="currentColor" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M16 12a4 4 0 10-8 0 4 4 0 008 0zm0 0v1.5a2.5 2.5 0 005 0V12a9 9 0 10-9 9m4.5-1.206a8.959 8.959 0 01-4.5 1.207" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"></path>
              </svg>
            </span>
          </div>
          <div class="input-container">
            <input id="authPass" placeholder="Digite sua senha" type="password" autocomplete="current-password" minlength="6" required>
            <span aria-hidden="true">
              <svg stroke="currentColor" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"></path>
                <path d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"></path>
              </svg>
            </span>
          </div>
          <button class="auth-submit" id="authLogin" type="submit">Entrar</button>
          <p class="signup-link">Primeiro acesso? <button class="signup-action" id="authSignup" type="button">Criar acesso</button></p>
          <div class="auth-msg" id="authMsg"></div>
        </form>
      </div>`;
    document.body.insertBefore(gate, document.body.firstChild);

    const form = gate.querySelector('#authForm');
    const email = gate.querySelector('#authEmail');
    const pass = gate.querySelector('#authPass');
    const msg = gate.querySelector('#authMsg');
    const login = gate.querySelector('#authLogin');
    const signup = gate.querySelector('#authSignup');

    const setBusy = v => { login.disabled = v; signup.disabled = v; };
    const show = (text, type) => { msg.textContent = text || ''; msg.className = 'auth-msg' + (type ? ' ' + type : ''); };

    form.addEventListener('submit', async e => {
      e.preventDefault();
      show('');
      setBusy(true);
      const em = emailNorm(email.value);
      const pw = pass.value;

      let result = await client.auth.signInWithPassword({ email: em, password: pw });

      if (result.error) {
        const code = String(result.error.code || '').toLowerCase();
        const message = String(result.error.message || '').toLowerCase();

        if (code === 'email_not_confirmed' || message.includes('email not confirmed')) {
          const created = await client.functions.invoke('create-app-user', {
            body: { email: em, password: pw }
          });
          if (!created.error) {
            result = await client.auth.signInWithPassword({ email: em, password: pw });
          }
        }
      }

      setBusy(false);
      if (result.error) {
        return show('E-mail ou senha inválidos. Confira os dados e tente novamente.', 'err');
      }
      location.reload();
    });

    signup.addEventListener('click', async () => {
      const em = emailNorm(email.value);
      const pw = pass.value;
      if (!em || pw.length < 6) {
        show('Informe um e-mail válido e uma senha com pelo menos 6 caracteres.', 'err');
        return;
      }

      show('');
      setBusy(true);

      const { data, error } = await client.functions.invoke('create-app-user', {
        body: { email: em, password: pw }
      });

      if (error || !data || data.error) {
        setBusy(false);
        return show((data && data.error) || 'Não foi possível criar o acesso.', 'err');
      }

      const loginResult = await client.auth.signInWithPassword({
        email: em,
        password: pw
      });

      setBusy(false);

      if (loginResult.error) {
        if (data.created === false) {
          return show('Este acesso já existe. Use a senha criada anteriormente e clique em Entrar.', 'err');
        }
        return show('Acesso criado, mas não foi possível entrar automaticamente. Clique em Entrar.', 'err');
      }

      location.reload();
    });
  }

  function setAppVisible(visible) {
    const wrap = document.querySelector('.wrap');
    if (wrap) wrap.hidden = !visible;
    const gate = document.getElementById('authGate');
    if (gate) gate.hidden = visible;
  }

  function showDenied(email) {
    injectAuthUI();
    setAppVisible(false);
    const gate = document.getElementById('authGate');
    gate.innerHTML = `
      <div class="auth-card auth-denied">
        <div class="auth-brand">Atlas</div>
        <strong>Acesso não liberado</strong>
        <p>O e-mail <b>${String(email || '')}</b> entrou no Supabase, mas ainda não está autorizado no Atlas.</p>
        <button class="auth-btn" id="authExit" type="button">Sair</button>
      </div>`;
    gate.querySelector('#authExit').onclick = async () => {
      await client.auth.signOut();
      location.reload();
    };
  }

  async function init() {
    injectAuthUI();
    const { data: { session }, error } = await client.auth.getSession();
    if (error || !session || !session.user) {
      setAppVisible(false);
      return { authenticated: false };
    }

    const email = emailNorm(session.user.email);
    const access = await accessFor(email);
    if (!access || access.active === false) {
      showDenied(email);
      return { authenticated: false, denied: true };
    }

    setAppVisible(true);
    return {
      authenticated: true,
      db,
      user: userAdapter,
      userId: session.user.id,
      email,
      role: access.role,
      client
    };
  }

  async function signOut() {
    await client.auth.signOut();
    location.reload();
  }

  window.froeseBackend = {
    client,
    db,
    init,
    signOut,
    syncAccess,
    removeAccess,
    linkPerson,
    accessFor
  };
})();